'''
Created on Nov 18, 2015

@author: natasha.gajic
'''
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
        today, time(15, 00, 0)) - timedelta(days=1),
        #today, time(8, 30, 0)),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),


}
TR = TriggerRule

dag = DAG('cloud-events-usage1', default_args=default_args, schedule_interval=timedelta(days=1))
script_folder = DAGS_FOLDER+'/../scripts/'
#dag = DAG('cloud-events-usage', default_args=default_args)
t1 = BashOperator(
    task_id='cloud-events-usage-load_orc1',
    #bash_command='/home/airflow/airflow-jobs/scripts/cloud_events_usage/backup_cloud_usage_events_with_orc1.sh;',
    bash_command=script_folder+'cloud_events_usage/backup_cloud_usage_events_with_orc1.sh;',
    dag=dag)
t2 = BashOperator(
    task_id='cloud-events-usage-load_orc',
    #bash_command='/home/airflow/airflow-jobs/scripts/cloud_events_usage/backup_cloud_usage_events_with_orc.sh;',
    bash_command=script_folder+'cloud_events_usage/backup_cloud_usage_events_with_orc.sh;',
    dag=dag)
t3 = BashOperator(
    task_id='cloud-events-usage-load_rawxml',
    #bash_command='/home/airflow/airflow-jobs/scripts/cloud_events_usage/backup_cloud_usage_events_rawxml.sh;',
    bash_command=script_folder+'cloud_events_usage/backup_cloud_usage_events_rawxml.sh;',
    dag=dag)
t4 = BashOperator(
    task_id='cloud-events-usage-load_rawxml1',
    #bash_command='/home/airflow/airflow-jobs/scripts/cloud_events_usage/backup_cloud_usage_events_rawxml1.sh;',
    bash_command=script_folder+'cloud_events_usage/backup_cloud_usage_events_rawxml1.sh;',
    dag=dag)
t5 = BashOperator(
    task_id='cloud-events-usage-load_glance_and_nova',
    #bash_command='/home/airflow/airflow-jobs/scripts/cloud_events_usage/backup_glance_and_nova.sh;',
    bash_command=script_folder+'cloud_events_usage/backup_glance_and_nova.sh;',
    dag=dag)
t6 = BashOperator(
    task_id='cloud-events-usage-verify-load',
    #bash_command='/home/airflow/airflow-jobs/scripts/cloud_events_usage/checkDailyLoad.sh;',
    bash_command=script_folder+'ods_archiving/checkDailyLoad.sh cloud_usage_events;',
    dag=dag, trigger_rule=TR.ALL_DONE)
t6.set_upstream(t1)
t6.set_upstream(t2)
t6.set_upstream(t3)
t6.set_upstream(t4)
t6.set_upstream(t5)
