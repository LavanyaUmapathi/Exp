usage="$(basename "$0") table_name index_column schema_name populate_orc_table_flag"
if [[ $1 == "-h" ]]
then
  echo $usage
  echo "The script loads cloud_usage_events tables from Microsoft SQL Server to Hadoop V2"
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
subject=$1
dateColumnName=$2
schema=$3
orcFlag=$4
if [[ $dry == "echo" ]]
then
  subject=$2
  dateColumnName=$3
  schema=$4
  orcFlag=$5
fi
orc="_orc"
subject_orc=$subject$orc
echo $subject
echo $dateColumnName
echo $subject_orc
if [[ $dry == "echo" ]]
then
  echo "Set start date in ./dates/$subject file"
fi
cdate=$(awk '{print $1}' ./dates/$subject)
cdatets=$(date -d "$cdate " +%s)
ndate=$(date -d "$cdate +1day" +%Y-%m-%d)
ndatets=$(date -d "$ndate " +%s)
targetdir="/apps/hive/warehouse/cloud_usage_events.db/$subject"
echo $cdate
echo $ndate
echo $targetdir
echo $cdatets
echo $ndatets
echo "****************"

currDate=$(date  +%Y-%m-%d)
echo $currDate

if [[ $dry == "echo" ]]
then
   echo "The following will be performed for each day up to the current date"
fi

$dry echo "ADD JAR  hdfs:/hdp/apps/2.2.4.2-2/hive/auxjars/csv-serde-1.1.2-0.11.0-all.jar;" > /tmp/scripts/$subject
$dry echo "set hive.stats.autogather=false;" >> /tmp/scripts/$subject
$dry echo "set hive.support.quoted.identifiers=none;" >> /tmp/scripts/$subject


while [[ ("$cdate" != "$currDate") && ("$cdate" < "$currDate") ]]
do
if [[ $dry == "echo" ]]
then
   echo "When processControlFlag file contains value 1 the process will exit"
fi

stopProcess=$(awk '{print $1}' ./processControlFlag)
if [[ "$stopProcess" == 1 ]]
then
  echo "Received signal to stop processing $subject"
  break
fi

echo "started to load $cdate"
newdir="$targetdir/date=$cdate"
echo $newdir
echo "Clean up old files"

if [[ -d "vw_$subject" ]]
then
  echo "Remove MR output   dir: vw_$subject"
  rmDir="rm -rf vw_$subject"
  $dry eval $rmDir
fi

rmHDFSCmd="sudo -u hdfs hadoop dfs -rm -r $newdir > /dev/null 2>&1"
echo $rmHDFSCmd
$dry eval $rmHDFSCmd &


sqoopcmd="sudo -u hdfs $SQOOP_HOME/bin/sqoop import --connect \"jdbc:sqlserver://GET_IP:1433;database=Cloud_Usage_Events;username=GET_USERNAME;password=GET_PASSWORD\"  --table vw_$subject --where \"$dateColumnName>='$cdate' and $dateColumnName<'$ndate'\" --fields-terminated-by , --escaped-by \\\ --enclosed-by '\"'  --compress -m 1 --target-dir $newdir -- --schema $schema "
echo $sqoopcmd
$dry eval $sqoopcmd
echo "script is created - running beeline NO set HADOOP_CLIENT_OPTS"
$dry echo "alter table cloud_usage_events.$subject drop if exists partition (date='$cdate');" >> /tmp/scripts/$subject
$dry echo "alter table cloud_usage_events.$subject add partition (date='$cdate');" >> /tmp/scripts/$subject
if [[ $orcFlag == "true" ]]
then
  $dry echo "insert OVERWRITE table cloud_usage_events.$subject$orc partition (date='$cdate') select \`(date)?+.+\`  from cloud_usage_events.$subject where date='$cdate';" >> /tmp/scripts/$subject
fi
echo "cmpleted load $cdate"
cdate=$(date -d "$cdate +1day" +%Y-%m-%d)
ndate=$(date -d "$ndate +1day" +%Y-%m-%d)
echo "new cdate $cdate; new ndate $ndate"
cdatets=$(date -d "$cdate " +%s)
ndatets=$(date -d "$ndate " +%s)

echo "new cdate $cdatets; new ndate $ndatets"
#exit
if [[ $dry == "echo" ]]
then
  echo "Exiting dry run"
  exit
fi
done
echo "exit;" >> /tmp/scripts/$subject
cat /tmp/scripts/$subject
hiveORCCmd="sudo -u hdfs $HIVE_HOME/bin/hive  -f /tmp/scripts/$subject"
echo $hiveORCCmd
$dry eval $hiveORCCmd  &
PID2=$!
echo "Hive load partitions PID is $PID2"
$dry wait $PID2
echo "Hive load partitions finished"
echo $cdate > dates/$subject
