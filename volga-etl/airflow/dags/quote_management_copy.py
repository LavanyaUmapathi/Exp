import os

from airflow import DAG
from airflow import DAGS_FOLDER
from airflow.models import Variable
from airflow.operators import BashOperator
from datetime import datetime, timedelta, time

from common.configs import get_config
from common.log import get_log_path
from common.tasks import alert_task
from common.safepass import get_safepass
from socket import gethostname

try:
    default_config = Variable.get("DEFAULT_CONFIG")
except:
    default_config = None

try:
    quotes_conf_path = Variable.get("quotes_conf_path")
except:
    if default_config:
        print 'quotes_conf_path not specified in Variables, using defaults.'
        quotes_conf_path = 'quote_management/staging.yaml'
    else:
        raise

print 'quotes_conf_path', quotes_conf_path
quotes_copy_config = get_config(quotes_conf_path)
mongossl= quotes_copy_config['MONGO_SSL']
mongodb_creds = get_safepass( quotes_copy_config['SAFEPASS_MONGO_PROJECT_ID'],
                              quotes_copy_config['SAFEPASS_MONGO_CRED_ID'])
postgres_creds = get_safepass( quotes_copy_config['SAFEPASS_POSTGRES_ODS_PROJECT_ID'],
                               quotes_copy_config['SAFEPASS_POSTGRES_ODS_CRED_ID'])
jenkins = quotes_copy_config['JENKINS']
jenkins_token = quotes_copy_config['JENKINS_TOKEN']
jenkins_mongo_hadoop_proj = quotes_copy_config['JENKINS_MONGO_HADOOP_PROJ']
jenkins_mongo_hive_mapping_proj = quotes_copy_config['JENKINS_MONGO_HIVE_MAPPING_PROJ']
jenkins_caspian_data_access_proj = quotes_copy_config['JENKINS_CASPIAN_DATA_ACCESS_PROJ']
mongo_hadoop_ver = quotes_copy_config['CUR_VER_MONGO_HADOOP']
hive_mapping_ver = quotes_copy_config['CUR_VER_MONGO_HIVE_MAPPING']
caspian_ver = quotes_copy_config['CUR_VER_CASPIAN_DATA_ACCESS']
hivehost = quotes_copy_config['HIVEHOST']
hiveport = quotes_copy_config['HIVEPORT']
hivedbname = quotes_copy_config['HIVEDBNAME']
mongodbname = quotes_copy_config['MONGO_DBNAME']
psqldbname = quotes_copy_config['PSQL_DBNAME']
ACTIVATE_VENV = 'source /usr/local/airflow-venv/bin/activate; '

if mongossl == 1:
    mongo_client_opts = '--ssl'
    mongo_hadoop_params = '?readPreference=secondary&ssl=true'
else:
    mongo_client_opts = ''
    mongo_hadoop_params = '?readPreference=secondary'

afwpath=DAGS_FOLDER
print 'DAGS_FOLDER', afwpath

mongo_hadoop_arc_path_no_ext=os.path.join(afwpath, jenkins_mongo_hadoop_proj)
mongo_hive_mapping_arc_path_no_ext=os.path.join(afwpath, jenkins_mongo_hive_mapping_proj)
caspian_data_access_arc_path_no_ext=os.path.join(afwpath, jenkins_caspian_data_access_proj)

today = datetime.today()
prev_date_str=(today - timedelta(days=1)).strftime("%Y/%m/%d")
start_date_str=today.strftime("%Y/%m/%d")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['yaroslav.litvinov@rackspace.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime.combine(today, time(16, 0, 0)) - timedelta(days=1,hours=0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

sched_repeat_days=1
sched_repeat_hours=0
dag = DAG('quotes',
          default_args=default_args,
          schedule_interval=timedelta(days=sched_repeat_days,
                                      hours=sched_repeat_hours))

#job: mongo-hadoop create csv files

#job copy to box

clean_cmd='rm -f {0}.tgz {1}.tgz {2}.tgz && rm -rf {0}'.format(caspian_data_access_arc_path_no_ext, 
                                                               mongo_hive_mapping_arc_path_no_ext,
                                                               mongo_hadoop_arc_path_no_ext)
pre_clean = BashOperator(task_id='pre-clean', bash_command = ACTIVATE_VENV + clean_cmd, dag=dag)

#get host in runtime instead of using config value
host = gethostname()
jenkins_job_fmt='%s/common/jenkins_job_scp.sh {projname} {authtoken} {repotag} %s {filepath} ' % (afwpath, host)
repo1_scp = BashOperator(task_id = 'jenkins_scp1', env={'JENKINS':jenkins}, bash_command \
                             = ACTIVATE_VENV + \
                             jenkins_job_fmt.format(projname=jenkins_mongo_hadoop_proj,
                                                    authtoken=jenkins_token,
                                                    repotag=mongo_hadoop_ver,
                                                    filepath=mongo_hadoop_arc_path_no_ext),
                         dag=dag)
repo2_scp = BashOperator(task_id = 'jenkins_scp2', env={'JENKINS':jenkins}, bash_command \
                             = ACTIVATE_VENV + \
                             jenkins_job_fmt.format(projname=jenkins_mongo_hive_mapping_proj,
                                                    authtoken=jenkins_token,
                                                    repotag=hive_mapping_ver,
                                                    filepath=mongo_hive_mapping_arc_path_no_ext),
                         dag=dag)

repo3_scp = BashOperator(task_id = 'jenkins_scp3', env={'JENKINS':jenkins}, bash_command \
                             = ACTIVATE_VENV + \
                             jenkins_job_fmt.format(projname=jenkins_caspian_data_access_proj,
                                                    authtoken=jenkins_token,
                                                    repotag=caspian_ver,
                                                    filepath=caspian_data_access_arc_path_no_ext),
                         dag=dag)


pre_build_fmt='\
mkdir -p {caspian} && \
mkdir -p {caspian}/proposal-manager/mongo-hadoop && \
mkdir -p {caspian}/proposal-manager/mongo_to_hive_mapping && \
tar -zxvf {caspian}.tgz -C {caspian} && \
tar -zxvf {mongo_hadoop}.tgz -C {caspian}/proposal-manager/mongo-hadoop && \
tar -zxvf {mongo_hive}.tgz -C {caspian}/proposal-manager/mongo_to_hive_mapping'

pre_build_cmd=pre_build_fmt.format(caspian=caspian_data_access_arc_path_no_ext, 
                                   mongo_hadoop=mongo_hadoop_arc_path_no_ext,
                                   mongo_hive=mongo_hive_mapping_arc_path_no_ext)

pre_build = BashOperator(task_id='pre-build', bash_command=ACTIVATE_VENV + pre_build_cmd, dag=dag)

cmd_fmt='%s/proposal-manager/{cmd}' % caspian_data_access_arc_path_no_ext

options_env = {
    'MONGO_DBNAME': str(mongodbname),
    'MONGO_CLIENT_PARAMS': str(mongo_client_opts),
    'MONGO_URI':  str(mongodb_creds[0]),
    'MONGO_USER': str(mongodb_creds[1]),
    'MONGO_PASS': str(mongodb_creds[2]),
    'PSQL_PORT': str(5432),
    'PSQL_DBNAME': str(psqldbname),
    'PSQL_URI': str(postgres_creds[0]),
    'PSQL_USER': str(postgres_creds[1]),
    'PSQL_PASS': str(postgres_creds[2]),
    'PSQL_SCHEMA': 'quote_mgmt_ods',
    'HIVE_DBNAME': str(hivedbname),
    'HIVE_SERVER_HOST': str(hivehost),
    'HIVE_SERVER_PORT': str(hiveport),
    'HIVE_TEZ_JAVA_OPTS': '-Xmx8192m',
    'HIVE_TEZ_CONTAINER_SIZE': '12000',
    'MONGO_HADOOP_PARAMS': str(mongo_hadoop_params),
    'START_DATE': start_date_str,
    'PREV_DATE': prev_date_str,
    'BASE_HDFS_FOLDER': '/QM/%s/' % start_date_str,
    'JAVA_HOME': '/etc/alternatives/java_sdk_openjdk'
}

#Be sure to add traling space after shell commands, to avoid jinja TemplateNotFound errors
build = BashOperator(task_id='build', env=options_env, dag=dag,
                     bash_command = ACTIVATE_VENV + \
                         cmd_fmt.format(cmd='build_set_mongo-hadoop_1_3_3.sh ') )

pre_work = BashOperator(task_id='prework', env=options_env, retries=1, dag=dag,
                        bash_command = ACTIVATE_VENV + \
                            cmd_fmt.format(cmd='generate_sql_statements.sh ') )

etl_work = BashOperator(task_id='etl_work', env=options_env, retries=3, dag=dag,
                        bash_command = ACTIVATE_VENV + \
                            cmd_fmt.format(cmd='workflow_start_resume_mongo.sh ') )

csv_prepare_work = BashOperator(task_id='csv_prepare_work', env=options_env, retries=3, dag=dag,
                        bash_command = ACTIVATE_VENV + \
                                    cmd_fmt.format(cmd='workflow_start_resume_csv.sh '))

final_work = BashOperator(task_id='final_work', env=options_env, retries=3, dag=dag,
                          bash_command = ACTIVATE_VENV + \
                              cmd_fmt.format(cmd='workflow_start_resume_export.sh ') )

#jobs dependencies
pre_clean.set_downstream([repo1_scp, repo2_scp, repo3_scp])
pre_build.set_upstream([repo1_scp, repo2_scp, repo3_scp])
build.set_upstream(pre_build)
pre_work.set_upstream(build)
etl_work.set_upstream(pre_work)
csv_prepare_work.set_upstream(etl_work)
final_work.set_upstream(csv_prepare_work)

#for test purposes
username=mongodb_creds[1]
