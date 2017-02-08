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
        today, time(20, 00, 0)) - timedelta(days=1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
TR = TriggerRule
dag=DAG('usl_ods_v1', default_args=default_args, schedule_interval=timedelta(days=1))
script_folder = DAGS_FOLDER+'/../scripts/'
t0=BashOperator(
    task_id='usldb_ods_incremental1',
    bash_command=script_folder+'usl_ods/usldb_ods_incremental1.sh;',
    dag=dag)
t1=BashOperator(
    task_id='usldb_ods_incremental2',
    bash_command=script_folder+'usl_ods/usldb_ods_incremental2.sh;',
    dag=dag)
t2=BashOperator(
    task_id='usldb_ods_full_load_all',
    bash_command=script_folder+'usl_ods/usldb_ods_full_load_all.sh;',
    dag=dag)
t3=BashOperator(
    task_id='verify_load',
    bash_command=script_folder+'ods_archiving/checkDailyLoad.sh usldb_ods;',
    dag=dag, trigger_rule=TR.ALL_DONE)
t3.set_upstream(t0)
t3.set_upstream(t1)
t3.set_upstream(t2)
