from airflow import DAG
from airflow.operators import BashOperator
from airflow.models import AirflowException, Variable
from datetime import datetime, timedelta, time

from common.configs import get_config
from common.log import get_log_path


RUN_AS = 'sudo env PATH=$PATH su -c "{{ params.path }}/{{ params.cmd }}" {{ params.user }}'


try:
    default_config = Variable.get("DEFAULT_CONFIG")
    common_config = get_config('common.yaml')
    task_config = get_config('monitoring_etl.yaml')
except AirflowException as error:
    raise RuntimeError("Can't use default config, no another configs")

LOG_PATH = get_log_path(
    common_config['LOGS'],
    task_config['LOG_PREFIX'])

today = datetime.today()
hour = 4
minute = 30
d_delta = 1
h_delta = 0
m_delta = 0

default_args = {
    'owner': task_config['user'],
    'depends_on_past': False,
    'start_date':datetime.combine(
        today, time(4, 00, 0)) - timedelta(days=1),
    'email': ['GET_HADOOP_SUPPORT@rackspace.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'queue': 'maas',
    'params': {
        'user': task_config['user'],
        'path': task_config['acumen_cron_scripts_dir']
    }
}

dag = DAG('monitoring_etl',
          default_args=default_args,
          schedule_interval=timedelta(days=1))

mad = BashOperator(task_id='maas_accounts_download',
                   bash_command=RUN_AS,
                   params={'cmd': 'cron-mad.sh'},
                   dag=dag)

mal = BashOperator(task_id='maas_accounts_load',
                   bash_command=RUN_AS,
                   params={'cmd': 'cron-mal.sh'},
                   dag=dag)
mal.set_upstream(mad)

mmd = BashOperator(task_id='maas_metrics_download',
                   bash_command=RUN_AS,
                   params={
                       'cmd': 'download_from_cf.sh',
                       'path': '/home/maas/maas_report'},
                   dag=dag)
mmd.set_upstream(mal)

mml = BashOperator(task_id='maas_metrics_load',
                   bash_command=RUN_AS,
                   params={'cmd': 'cron-mml.sh'},
                   dag=dag)
mml.set_upstream(mmd)

mnd = BashOperator(task_id='maas_notifications_download',
                   bash_command=RUN_AS,
                   params={
                       'cmd': 'download_notification_from_cf.sh',
                       'path': '/home/maas/maas_report'},
                   dag=dag)
mnd.set_upstream(mml)

mnl = BashOperator(task_id='maas_notifications_load',
                   bash_command=RUN_AS,
                   params={'cmd': 'cron-mnl.sh'},
                   dag=dag)
mnl.set_upstream(mnd)

nri = BashOperator(task_id='newrelic_ingester',
                   bash_command=RUN_AS,
                   params={
                       'cmd': 'newrelic_ingester.py {{ ds }}',
                       'path': task_config['acumen_python_dir']},
                   dag=dag)

nrml = BashOperator(task_id='newrelic_monitors_load',
                    bash_command=RUN_AS,
                    params={
                        'cmd': 'newrelic-monitors-load-date.py {{ ds }}',
                        'path': task_config['acumen_scripts_dir'] + '/newrelic-etl'},
                    dag=dag)
nrml.set_upstream(nri)

nrpl = BashOperator(task_id='newrelic_polls_load',
                    bash_command=RUN_AS,
                    params={
                        'cmd': 'newrelic-polls-load-date.py {{ ds }}',
                        'path': task_config['acumen_scripts_dir'] + '/newrelic-etl'},
                    dag=dag)
nrpl.set_upstream(nri)

mrp = BashOperator(task_id='maas_report_prep',
                   bash_command=RUN_AS,
                   params={
                       'cmd': 'prep_report.sh',
                       'path': '/home/maas/maas_report'},
                   dag=dag)
mrp.set_upstream([mnl, nrml, nrpl])

nrrp = BashOperator(task_id='newrelic_report_prep',
                    bash_command=RUN_AS,
                    params={
                        'cmd': 'prep_report_new_relic.sh',
                        'path': '/home/maas/maas_report'},
                    dag=dag)
nrrp.set_upstream(mrp)
