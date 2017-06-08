'''
Created on Apr 13, 2016

@author: natasha.gajic
'''
BASE_URL="http://admin1.prod.iad.caspian.rax.io:50070/"
USER_NAME="natasha"
HDFS_PATH='/apps/hive/warehouse/'
HIVE_HOST='admin1.prod.iad.caspian.rax.io'
#HIVE_HOST='admin2.prod.iad.caspian.rax.io'
HIVE_PORT=10000
#HIVE_PORT=9083
SQL_CONNECTION_FILE = '/var/run/ods_archive/.sqlconn'
WEB_BASE_URL='http://localhost:50111/templeton/v1/'
QUERY_URL='http://localhost:50111/templeton/v1/hive'
WEB_BASE_URL='http://admin2.prod.iad.caspian.rax.io:50111/templeton/v1/'
QUERY_URL='http://admin2.prod.iad.caspian.rax.io:50111/templeton/v1/hive'
WEBHCAT_USERNAME='natasha'
AIRFLOW_JOBS_HOME="/home/airflow/airflow-jobs"
FULL_TABLE_LOAD_SCRIPT="ods_full_load.sh"
FULL_PARTITIONED_TABLE_LOAD_SCRIPT="ods_full_partitioned_table_load.sh"
INCREMETAL_PARTITIONED_TABLE_LOAD_SCRIPT="ods_incremental_partitioned_table_load.sh"
INCREMENTAL_NON_PARTITION_TABLE_LOAD_SCRIPT="ods_incremental_non_partitoned_table_load.sh"
EMAIL_FROM="GET_HADOOP_SUPPORT@rackspace.com"
EMAIL_TO="GET_HADOOP_SUPPORT@rackspace.com"
ADD_JAR="ADD JAR  hdfs:/hdp/apps/2.2.4.2-2/hive/auxjars/csv-serde-1.1.2-0.11.0-all.jar"
COMMENT_SIMBOL="#"
RERUN_DIR="/home/airflow/airflow-jobs/scripts/ods_archiving/rerun"