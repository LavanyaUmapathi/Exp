from airflow import DAG
from airflow.operators import BashOperator
from airflow.models import AirflowException, Variable
from datetime import datetime, timedelta, time

from common.configs import get_config
from common.log import get_log_path



RUN_AS = 'sudo env PATH=$PATH su -c "{path}/{cmd}" {user}'

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

default_user = task_config['user']
default_path = task_config['acumen_cron_scripts_dir']
maas_etl_path = task_config['maas_etl_dir']

default_args = {
    'owner': default_user,
    'depends_on_past': False,
    'start_date':datetime.combine(
        today, time(4, 00, 0)) - timedelta(days=1),
    'email': ['GET_HADOOP_SUPPORT@rackspace.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'queue': 'maas'
}

def sudo(cmd, path=default_path, user=default_user):
    return RUN_AS.format(path=path, cmd=cmd, user=user)

dag = DAG('monitoring_etl',
          default_args=default_args,
          schedule_interval=timedelta(days=1))

mad = BashOperator(task_id='maas_accounts_download',
                   bash_command=sudo('cron-mad.sh'),
                   dag=dag)

mal = BashOperator(task_id='maas_accounts_load',
                   bash_command=sudo('cron-mal.sh'),
                   dag=dag)
mal.set_upstream(mad)

mml = BashOperator(task_id='maas_metrics_load',
                   bash_command=sudo('cron-mml.sh'),
                   dag=dag)
mml.set_upstream(mal)

mnd = BashOperator(task_id='maas_notifications_download',
                   bash_command=sudo('download_notification_from_cf.sh',
                                     path=maas_etl_path),
                   dag=dag)
mnd.set_upstream(mml)

mnl = BashOperator(task_id='maas_notifications_load',
                   bash_command=sudo('cron-mnl.sh'),
                   dag=dag)
mnl.set_upstream(mnd)

nri = BashOperator(task_id='newrelic_ingester',
                   bash_command=sudo('newrelic_ingest.py {{ ds }}',
                                     path=task_config['acumen_scripts_dir']),
                   dag=dag)

nrml = BashOperator(task_id='newrelic_monitors_load',
                    bash_command=sudo('newrelic-monitors-load-date.sh {{ ds }}',
                                      path=task_config['acumen_admin_dir'] + '/newrelic-etl'),
                    dag=dag)
nrml.set_upstream(nri)

nrpl = BashOperator(task_id='newrelic_polls_load',
                    bash_command=sudo('newrelic-polls-load-date.sh {{ ds }}',
                                      path=task_config['acumen_admin_dir'] + '/newrelic-etl'),
                    dag=dag)
nrpl.set_upstream(nri)

mrp = BashOperator(task_id='maas_report_prep',
                   bash_command=sudo('prep_report.sh',
                                     path='/home/maas/metric_report'),
                   dag=dag)
mrp.set_upstream([mnl, nrml, nrpl])

nrrp = BashOperator(task_id='newrelic_report_prep',
                    bash_command=sudo('prep_report_new_relic.sh',
                                      path='/home/maas/metric_report'),
                    dag=dag)
nrrp.set_upstream(mrp)
acalc = BashOperator(task_id='availability_calculation',
                    bash_command=sudo('up_time_calculation.sh',
                                      path='/home/maas/metric_report'),
                    dag=dag)
acalc.set_upstream(nrrp)
