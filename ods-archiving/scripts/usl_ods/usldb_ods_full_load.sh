usage="$(basename "$0") table_name schema_name "
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
export AIRFLOW_RUN_DIR=/home/airflow/airflow-jobs/script
export RUN_DIR=/var/run/ods_archive
export JAVA_HOME=/usr/lib/jvm/jre-openjdk

sql_ip=$(head -n 1 $RUN_DIR//.sqlconn)
sql_u_name=$(head -n 2 $RUN_DIR/.sqlconn | tail -n 1)
sql_p_word=$(tail -n 1 $RUN_DIR/.sqlconn)

subject=$1
schema=$2
if [[ $dry == "echo" ]]
then
  subject=$2
  schema=$3
fi
echo $subject
targetdir="/apps/hive/warehouse/usldb_ods.db/$subject"
echo $targetdir
echo "****************"

echo "Truncate table"
truncateCmd="sudo -u ods_archive hive -e 'truncate table usldb_ods.$subject'"
echo $truncateCmd
$dry eval $truncateCmd &
PID2=$!
echo "Hive  truncate table PID is $PID2"
$dry wait $PID2


if [[ $dry == "echo" ]]
then
   echo "When processControlFlag file contains value 1 the process will exit"
fi

stopProcess=$(awk '{print $1}'  $RUN_DIR/usl_ods/processControlFlag)
if [[ "$stopProcess" == 1 ]]
then
  echo "Received signal to stop processing $subject"
  break
fi

newdir="$targetdir"
echo $newdir
#rmHDFSCmd="sudo -u hdfs hadoop dfs -rm -r $newdir > /dev/null 2>&1"
#echo $rmHDFSCmd
#$dry eval $rmHDFSCmd &


sqoopcmd="sudo -u ods_archive $SQOOP_HOME/bin/sqoop import --connect \"jdbc:sqlserver://$sql_ip;database=usldb_ods;username=$sql_u_name;password=$sql_p_word\"  --table vw_$subject  --fields-terminated-by , --escaped-by \\\ --enclosed-by '\"'    --compress -m 1 --target-dir $newdir --append  -- --schema $schema "
echo $sqoopcmd
$dry eval $sqoopcmd
#exit
if [[ $dry == "echo" ]]
then
  echo "Exiting dry run"
  exit
fi
