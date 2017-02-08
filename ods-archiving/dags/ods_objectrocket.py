from airflow import DAG
from airflow import DAGS_FOLDER
from airflow.operators import BashOperator

from datetime import datetime, timedelta, time
from airflow.utils import TriggerRule

today = datetime.today()

default_args = { 
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':datetime.combine(
        today, time(13, 00, 0)) - timedelta(days=1),
    'email': ['GET_HADOOP_SUPPORT@rackspace.com'],
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
TR = TriggerRule
dag=DAG('ods_objectrocket', default_args=default_args, schedule_interval=timedelta(days=1))
script_folder = DAGS_FOLDER+'/../scripts/'
t0=BashOperator(
    task_id='ods_load_batch_0',
    bash_command=script_folder+'ods_objectrocket/ods_load_batch_0.sh;',
    dag=dag)
t1=BashOperator(
    task_id='ods_load_batch_1',
    bash_command=script_folder+'ods_objectrocket/ods_load_batch_1.sh;',
    dag=dag)
t5=BashOperator(
    task_id='verify_load',
    bash_command=script_folder+'ods_archiving/checkDailyLoad.sh ods_objectrocket;',
    dag=dag, trigger_rule=TR.ALL_DONE)
t5.set_upstream(t0)
t5.set_upstream(t1)
