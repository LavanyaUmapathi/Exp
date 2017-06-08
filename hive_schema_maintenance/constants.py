'''
Created on Feb 25, 2016

@author: natasha.gajic
'''
BASE_URL = 'http://localhost:50111/templeton/v1/'
QUERY_URL = 'http://localhost:50111/templeton/v1/hive'
WEBHCAT_USERNAME = 'ods_archive'
DEFAULT_SERDE = 'com.bizo.hive.serde.csv.CSVSerde'
ORC_SERDE = 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
ORC_INPUT_FORMAT = 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
ORC_OUTPUT_FORMAT = 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
DEFAULT_FORMAT = 'org.apache.hadoop.mapred.TextInputFormat'
DEFAULT_FORMAT_DICT = {
    "storedAs": "textfile",
    "rowFormat": {
        "serde": {"name": "\"com.bizo.hive.serde.csv.CSVSerde\""}
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
REMOVE_DUPLICATES_SCRIPT='get_last_records.sh'
LATEST_TABLE_NAME='_latest'
CURRENT_TABLE_NAME='_current'
INVALID_COLUMN_NAMES=['date','text','when','order','binary','row','column','timestamp','end']

SHELL_SCRIPT_BASE_NAME = 'ods_load_batch'

ONE_TIME_LOAD_SHELL_SCRIPT = 'ods_one_time_load_tables.sh'

COMMENT_LINE = '#'

AIRFLOW_SCRIPTS_DIR = '/home/airflow/airflow-jobs/scripts/ods_archiving'

DAG_TEMPLATE = """
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

dag = DAG('{{ ods_name }}', 
          default_args=default_args, 
          schedule_interval=timedelta(days=1))

script_folder = DAGS_FOLDER + '/../scripts/'

tv = BashOperator(task_id='verify_load',
                  bash_command=script_folder + 'ods_archiving/checkDailyLoad.sh {{ ods_name }};',
                  dag=dag, 
                  trigger_rule=TriggerRule.ALL_DONE)
{% for i in range(split_cnt) %}
BashOperator(task_id='ods_load_batch_{{ i }}',
             bash_command=script_folder + '{{ ods_name }}/{{ script_name }}_{{ i }}.sh;',
             dag=dag).set_downstream(tv)
{% endfor %}
"""
