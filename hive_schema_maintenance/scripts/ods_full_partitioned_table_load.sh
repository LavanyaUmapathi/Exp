usage="$(basename "$0") db_name table_name date_field schema_name orc_flag isdate istimestamp"
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
extract_field=$3
schema=$4
orc_flag=$5
isdate=$6
istimestamp=$7
if [[ $dry == "echo" ]]
then
  db_name=$2
  subject=$3
  extract_field=$4
  schema=$5
  orc_flag=$6
  isdate=$7
  istimestamp=$8

fi
sql_ip=$(head -n 1 $RUN_DIR/.sqlconn)
sql_u_name=$(head -n 2 $RUN_DIR/.sqlconn | tail -n 1)
sql_p_word=$(tail -n 1 $RUN_DIR/.sqlconn)


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
current="_current"
currDate=$(date  +%Y-%m-%d)
echo $currDate
previousDate=$(date -d "$currDate -1day"  +%Y-%m-%d)
echo $previousDate
echo $subject
targetdir="/apps/hive/warehouse/$db_name.db/$subject/dt=$previousDate"
echo $targetdir
echo "****************"
rmHDFSCmd="sudo -u ods_archive hadoop dfs -rm -r $targetdir > /dev/null 2>&1"
echo $rmHDFSCmd
$dry eval $rmHDFSCmd &


echo $newdir

$dry echo "ADD JAR  hdfs:/hdp/apps/2.2.4.2-2/hive/auxjars/csv-serde-1.1.2-0.11.0-all.jar;" > /tmp/scripts/$subject
$dry echo "set hive.stats.autogather=false;" >> /tmp/scripts/$subject
$dry echo "set hive.support.quoted.identifiers=column;" >> /tmp/scripts/$subject


sqoopcmd="sudo -u ods_archive $SQOOP_HOME/bin/sqoop import --connect \"jdbc:sqlserver://$sql_ip;database=$db_name;username=$sql_u_name;password=$sql_p_word\"  --table $subject  --fields-terminated-by , --escaped-by \\\ --enclosed-by '\"'    --compress -m 1 --target-dir $targetdir --append  --hive-drop-import-delims -- --schema $schema  --table-hints NOLOCK"
echo $sqoopcmd
$dry eval $sqoopcmd
$dry echo "alter table $db_name.$subject drop if exists partition (dt='$previousDate');" >> /tmp/scripts/$subject
$dry echo "alter table $db_name.$subject add partition (dt='$previousDate');" >> /tmp/scripts/$subject

if [[ $orc_flag == "true" ]]
then
    orc="_orc"
    $dry echo "set hive.support.quoted.identifiers=none;" >> /tmp/scripts/$subject
    $dry echo "insert OVERWRITE table $db_name.$subject$orc partition (dt='$previousDate') select \`(dt)?+.+\`  from $db_name.$subject where dt='$previousDate';" >> /tmp/scripts/$subject
    $dry echo "insert OVERWRITE table  $db_name.$subject$current select \`(dt)?+.+\`  from $db_name.$subject$orc where dt='$previousDate';" >> /tmp/scripts/$subject
    $dry echo "set hive.support.quoted.identifiers=column;" >> /tmp/scripts/$subject
else
    $dry echo "set hive.support.quoted.identifiers=none;" >> /tmp/scripts/$subject
    $dry echo "insert OVERWRITE table  $db_name.$subject$current select \`(dt)?+.+\`  from $db_name.$subject where dt='$previousDate';" >> /tmp/scripts/$subject
    $dry echo "set hive.support.quoted.identifiers=column;" >> /tmp/scripts/$subject
fi
#exit
echo "exit;" >> /tmp/scripts/$subject
cat /tmp/scripts/$subject
chmod 755 /tmp/scripts/$subject
hiveORCCmd="sudo -u ods_archive $HIVE_HOME/bin/hive  -f /tmp/scripts/$subject"
echo $hiveORCCmd
$dry eval $hiveORCCmd  &
PID2=$!
echo "Hive load partitions PID is $PID2"
$dry wait $PID2
echo "Hive load partitions finished"

if [[ $dry == "echo" ]]
then
  echo "Exiting dry run"
  exit
fi
