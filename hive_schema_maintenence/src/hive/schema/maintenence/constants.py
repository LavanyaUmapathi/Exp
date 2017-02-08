'''
Created on Feb 25, 2016

@author: natasha.gajic
'''
BASE_URL = 'http://localhost:50111/templeton/v1/'
QUERY_URL = 'http://localhost:50111/templeton/v1/hive'
WEBHCAT_USERNAME = 'natasha'
DEFAULT_SERDE = 'com.bizo.hive.serde.csv.CSVSerde'
ORC_SERDE = 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
ORC_INPUT_FORMAT = 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
ORC_OUTPUT_FORMAT = 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
DEFAULT_FORMAT = 'org.apache.hadoop.mapred.TextInputFormat'
DEFAULT_FORMAT_DICT = {
    "storedAs": "textfile",
    "rowFormat": {
        "serde": {"name": "\u0022com.bizo.hive.serde.csv.CSVSerde\u0022"}
    }
}
ORC_FORMAT_DICT = {"storedAs": "orcfile"}

DW_TIMESTAMP = 'dw_timestamp'

TABLE_KEYWORD = 'TABLE'
HIVE_STRING_DATATYPE = 'String'
DATE_PARTITION_COL_NAME = 'dt'
SPLIT_NUMBER = 5
FULL_LOAD_SHELL_SCRIPT = 'ods_full_load.sh'
PARTITION_INCREMENTAL_LOAD_SCRIPT = 'ods_incremental_partitioned_table_load.sh'
NON_PARTITION_INCREMENTAL_LOAD_SCRIPT = 'ods_incremental_non_partitoned_table_load.sh'
FULL_LOAD_PARTITIONED_TABLE_SCRIPT = 'ods_full_partitioned_table_load.sh'

SHELL_SCRIPT_BASE_NAME = 'ods_load_batch_{i}.sh'

ONE_TIME_LOAD_SHELL_SCRIPT = 'ods_one_time_load_tables.sh'

COMMENT_LINE = '#'

AIRFLOW_SCRIPTS_DIR = '/home/airflow/airflow-jobs/scripts/ods_archiving'

DAG_HEADER = """from airflow import DAG
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
"""

DAG_NAME = 'dag=DAG(\'{ods_name}\', default_args=default_args, schedule_interval=timedelta(days=1))\n\
script_folder = DAGS_FOLDER+\'/../scripts/\'\n'

DAG_TASK_BASE_NAME = 't{i}'

DAG_TASK = 'BashOperator(\n\
    task_id=\'{task_name}\',\n\
    bash_command=script_folder+\'{ods_name}/{script_name};\',\n\
    dag=dag)\n'
DAG_VERIFY_TASK = 'BashOperator(\n\
    task_id=\'verify_load\',\n\
    bash_command=script_folder+\'ods_archiving/checkDailyLoad.sh {ods_name};\',\n\
    dag=dag, trigger_rule=TR.ALL_DONE)\n'
DAG_ORDER_STRING = 'set_upstream({task_id})\n'
