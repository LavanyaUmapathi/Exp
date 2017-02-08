usage="$(basename "$0")"
if [[ $1 == "-h" ]]
then
  echo $usage
  echo "The script collect data from up time calculation and execute calculation"
  echo "Dry run available as:  $(basename "$0") -dry"
  exit
fi
dry=

if [[ $1 == "-dry" ]]
then
   dry="echo"

fi
export RUN_DIR=/var/run/airflow
export SQOOP_HOME=/usr/hdp/current/sqoop-client
export HIVE_HOME=/usr/hdp/current/hive-client
export HADOOP_HOME=/usr/hdp/current/hadoop-client
export AIRFLOW_RUN_DIR=/home/airflow/airflow-jobs/scripts
sql_ip=$(head -n 1 $RUN_DIR/.sqlconn)
sql_u_name=$(head -n 2 $RUN_DIR/.sqlconn | tail -n 1)
sql_p_word=$(tail -n 1 $RUN_DIR/.sqlconn)

sqoopcmd="sqoop import --connect \"jdbc:sqlserver://$sql_ip;database=Operational_reporting_CORE;username=$sql_u_name;password=$sql_p_word\" --table server_status_transition_log  --compress -m 1 --target-dir /apps/hive/warehouse/up_time_calculation.db/server_status_transition_log --delete-target-dir --hive-drop-import-delims -- --schema CORE_Prod  --table-hints NOLOCK"
echo $sqoopcmd
$dry eval $sqoopcmd
suppressioncmd="spark-submit --master yarn  device_status_to_suppression.py"
echo $suppressioncmd
$dry eval $suppressioncmd
#servicedowntimecmd="spark-submit --master yarn service_downtime.py"
servicedowntimecmd="spark-submit --master yarn service_availability.py"
echo $servicedowntimecmd
$dry eval $servicedowntimecmd
mdowntimecmd="spark-submit --master yarn monitor_availability.py"
echo $mdowntimecmd
$dry eval $mdowntimecmd
