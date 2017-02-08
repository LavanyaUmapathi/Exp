usage="$(basename "$0") table_name index_column schema_name populate_orc_table_flag"
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

#set -e
export SQOOP_HOME=/usr/hdp/current/sqoop-client
export HIVE_HOME=/usr/hdp/current/hive-client
export HADOOP_HOME=/usr/hdp/current/hadoop-client
export AIRFLOW_RUN_DIR=/home/airflow/airflow-jobs/scripts
export RUN_DIR=/var/run/ods_archive

sql_ip=$(head -n 1 $RUN_DIR//.sqlconn)
sql_u_name=$(head -n 2 $RUN_DIR/.sqlconn | tail -n 1)
sql_p_word=$(tail -n 1 $RUN_DIR/.sqlconn)


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
if [[ -e $RUN_DIR/usl_ods/dates/$subject ]]
then
   echo "Date file exists"
else
   echo "Create start date file!"
   exit 1
fi
#Recover previous run if necessary
echo "Recover Start"
if [[ -e /tmp/scripts/$subject ]]
then
  echo "Recovery process"
  lline=$(awk 'END{gsub(/\n/,"",$0); print $0}' /tmp/scripts/$subject)
  echo "Last line is $lline"
  if [[ $lline == "exit;" ]]
  then
    echo "Previous process completed check date"
    lastdate=$(tail -n 2 /tmp/scripts/$subject | head -n 1 | awk '{print substr($0,length($0)-12, 10)}')
    if date -d $lastdate +%Y-%m-%d 2>&1 > /dev/null
    then
        echo "$lastdate is date"
        setdate=$(awk 'gsub(/\n/,"",$1);{print $1}' $RUN_DIR/usl_ods/dates/$subject)
        nextlastdate=$(date -d "$lastdate +1day" +%Y-%m-%d)
        echo $setdate
        echo $nextlastdate
        if [[ $nextlastdate == $setdate ]]
        then
           echo "Next date is set"
        else
           echo "Setting next date"
           echo $nextlastdate > $RUN_DIR/usl_ods/dates/$subject
        fi
    else
        echo "$lastdate is not date"
   fi
  else
    echo $lline
    lastdate=$(echo $lline |awk '{print substr($0,length($0)-12, 10)}')
    echo $lastdate
    date -d $lastdate  2>&1 > /dev/null
    if [[ $? ==  0 ]]
    then
        nextlastdate=$(date -d "$lastdate +1day" +%Y-%m-%d)
        echo "Next date is $nextlastdate"
        hiveORCCmd="sudo -u ods_archive $HIVE_HOME/bin/hive  -f /tmp/scripts/$subject"
        echo $hiveORCCmd
        $dry eval $hiveORCCmd  &
        PID2=$!
        echo "Hive load partitions PID is $PID2"
        $dry wait $PID2
        echo "Hive load partitions finished"
        echo $nextlastdate > $RUN_DIR/usl_ods/dates/$subject
    fi
  fi
fi
echo "Recovery finished"
set -e
if [[ $dry == "echo" ]]
then
  echo "Set start date in ./dates/$subject file"
fi
cdate=$(awk '{print $1}' $RUN_DIR/usl_ods/dates/$subject)
cdatets=$(date -d "$cdate " +%s)
ndate=$(date -d "$cdate +1day" +%Y-%m-%d)
ndatets=$(date -d "$ndate " +%s)
targetdir="/apps/hive/warehouse/usldb_ods.db/$subject"
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
$dry echo "set hive.support.quoted.identifiers=column;" >> /tmp/scripts/$subject


while [[ ("$cdate" != "$currDate") && ("$cdate" < "$currDate") ]]
do
if [[ $dry == "echo" ]]
then
   echo "When processControlFlag file contains value 1 the process will exit"
fi

stopProcess=$(awk '{print $1}' $RUN_DIR/usl_ods/processControlFlag)
if [[ "$stopProcess" == 1 ]]
then
  echo "Received signal to stop processing $subject"
  break
fi

echo "started to load $cdate"
newdir="$targetdir/dt=$cdate"
echo $newdir
echo "Clean up old files"

if [[ -d "vw_$subject" ]]
then
  echo "Remove MR output   dir: vw_$subject"
  rmDir="rm -rf vw_$subject"
  $dry eval $rmDir
fi

rmHDFSCmd="sudo -u ods_archive hadoop dfs -rm -r $newdir > /dev/null 2>&1"
echo $rmHDFSCmd
$dry eval $rmHDFSCmd &


sqoopcmd="sudo -u ods_archive $SQOOP_HOME/bin/sqoop import --connect \"jdbc:sqlserver://$sql_ip;database=usldb_ods;username=$sql_u_name;password=$sql_p_word\"  --table vw_$subject --where \"$dateColumnName>='$cdate' and $dateColumnName<'$ndate'\" --fields-terminated-by , --escaped-by \\\ --enclosed-by '\"'    --compress -m 1 --target-dir $newdir --hive-drop-import-delims -- --schema $schema "
echo $sqoopcmd
$dry eval $sqoopcmd
echo "script is created - running beeline NO set HADOOP_CLIENT_OPTS"
$dry echo "alter table usldb_ods.$subject drop if exists partition (dt='$cdate');" >> /tmp/scripts/$subject
$dry echo "alter table usldb_ods.$subject add partition (dt='$cdate');" >> /tmp/scripts/$subject
if [[ $orcFlag == "true" ]]
then
    $dry echo "set hive.support.quoted.identifiers=none;" >> /tmp/scripts/$subject
    $dry echo "insert OVERWRITE table usldb_ods.$subject$orc partition (dt='$cdate') select dt?+.+  from usldb_ods.$subject where dt='$cdate';" >> /tmp/scripts/$subject
    $dry echo "set hive.support.quoted.identifiers=column;" >> /tmp/scripts/$subject

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
hiveORCCmd="sudo -u ods_archive $HIVE_HOME/bin/hive  -f /tmp/scripts/$subject"
echo $hiveORCCmd
$dry eval $hiveORCCmd  &
PID2=$!
echo "Hive load partitions PID is $PID2"
$dry wait $PID2
echo "Hive load partitions finished"
echo $cdate > $RUN_DIR/usl_ods/dates/$subject

