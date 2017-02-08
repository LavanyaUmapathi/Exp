usage="$(basename "$0") db_name table_name schema_name "
if [[ $1 == "-h" ]]
then
  echo $usage
  echo "The script loads usldb_ods tables from Microsoft SQL Server to Hadoop V2"
  echo "Dry run available as:  $(basename "$0") -dry"
  exit
fi
dry=

if [[ $1 == "-dry" ]]
then
   dry="echo"

fi

set -e
export SQOOP_HOME=/usr/hdp/current/sqoop-client
export HIVE_HOME=/usr/hdp/current/hive-client
export HADOOP_HOME=/usr/hdp/current/hadoop-client
export AIRFLOW_RUN_DIR=/home/airflow/airflow-jobs/scripts
export RUN_DIR=/var/run/ods_archive
db_name=$1
subject=$2
schema=$3
orc_flag=$4
sql_ip=$(head -n 1 $RUN_DIR/.sqlconn)
sql_u_name=$(head -n 2 $RUN_DIR/.sqlconn | tail -n 1)
sql_p_word=$(tail -n 1 $RUN_DIR/.sqlconn)
echo $sql_ip
echo $sql_u_name
echo $sql_p_word
echo $orc_flag
if [[ $dry == "echo" ]]
then
  db_name=$2
  subject=$3
  schema=$4
  orc_flag=$5
fi

if [[ $dry == "echo" ]]
then
   echo "When processControlFlag file contains value 1 the process will exit"
fi

stopProcess=$(awk '{print $1}' $RUN_DIR/$db_name/processControlFlag)
if [[ "$stopProcess" == 1 ]]
then
  echo "Received signal to stop processing $subject"
  exit
fi


echo $subject
targetdir="/apps/hive/warehouse/$db_name.db/$subject"
echo $targetdir
echo "****************"

echo "Truncate table"
truncateCmd="sudo -u ods_archive hive -e 'truncate table $db_name.$subject'"
echo $truncateCmd
$dry eval $truncateCmd &
PID2=$!
echo "Hive  truncate table PID is $PID2"
$dry wait $PID2


echo $newdir


sqoopcmd="sudo -u ods_archive $SQOOP_HOME/bin/sqoop import --connect \"jdbc:sqlserver://$sql_ip;database=$db_name;username=$sql_u_name;password=$sql_p_word\"  --table $subject  --fields-terminated-by , --escaped-by \\\ --enclosed-by '\"'    --compress -m 1 --target-dir $targetdir --append  --hive-drop-import-delims -- --schema $schema  --table-hints NOLOCK"
echo $sqoopcmd
$dry eval $sqoopcmd
if [[ $orc_flag == true* ]]
then
    GLOBIGNORE="*"
    orc="_orc"
    hiveORCCmd="sudo -u ods_archive $HIVE_HOME/bin/hive  -e 'ADD JAR  hdfs:/hdp/apps/2.2.4.2-2/hive/auxjars/csv-serde-1.1.2-0.11.0-all.jar;set hive.stats.autogather=false;set hive.support.quoted.identifiers=column; insert overwrite table $db_name.$subject$orc select * from $db_name.$subject;'"
    echo $hiveORCCmd
    $dry eval $hiveORCCmd  &
    PID2=$!
    echo "Hive load orc PID is $PID2"
    $dry wait $PID2
    echo "Hive load orc finished"
fi
#exit
if [[ $dry == "echo" ]]
then
  echo "Exiting dry run"
  exit
fi
