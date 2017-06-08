from airflow import DAG
from airflow.operators import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta, time

from common.configs import get_config
from common.log import get_log_path
from common.tasks import alert_task

try:
    default_config = Variable.get("DEFAULT_CONFIG")
except:
    default_config = None

if default_config:
    common_config = get_config('common.yaml')
    task_config = get_config('cron_hdfs_uploader_stacktach_preprod.yaml')
else:
    raise Exception("Can't use default config, no another configs")

LOG_PATH = get_log_path(
    common_config['LOGS'],
    task_config['LOG_PREFIX'])

today = datetime.today()
hour = 1
minute = 25
d_delta = 1
h_delta = 0
m_delta = 0

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.combine(
        today, time(hour, minute)) - timedelta(days=d_delta,
                                               hours=h_delta,
                                               minutes=m_delta),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('cron_hdfs_uploader_stacktach_preprod',
          default_args=default_args,
          schedule_interval=timedelta(days=d_delta,
                                      hours=h_delta,
                                      minutes=m_delta))


command = 'sudo env PATH=$PATH su -c "{acumen_cron_scripts_dir}/cron-hdfs-uploader-stacktach-preprod.sh" {user}'.format(acumen_cron_scripts_dir=task_config['acumen_cron_scripts_dir'], user=task_config['user'])

t1 = BashOperator(task_id='cron_hdfs_uploader_stacktach_preprod',
                  bash_command=command, dag=dag)


t2 = alert_task(log=LOG_PATH,
                prefix=task_config['LOG_PREFIX'],
                dag=dag)

t2.set_upstream(t1)
