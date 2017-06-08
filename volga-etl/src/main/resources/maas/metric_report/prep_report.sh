usage="$(basename "$0")"
if [[ $1 == "-h" ]]
then
  echo $usage
  echo "The script runs up time calculation hql scripts"
  echo "Dry run available as:  $(basename "$0") -dry"
  exit
fi
dry=
if [[ $1 == "-dry" ]]
then
   dry="echo"
fi
export AIRFLOW_RUN_DIR=/home/airflow/airflow-jobs/scripts
export RUN_DIR=/home/maas/metric_report
export HIVE_HOME=/usr/hdp/current/hive-client
export HADOOP_HOME=/usr/hdp/current/hadoop-client

if [[ -f $RUN_DIR/lock.file ]]
then
        echo "Report process is running - will not run"
        exit
fi
touch $RUN_DIR/lock.file
cdate=$(awk '{print $1}' $RUN_DIR/maas_date)
echo $cdate
cmd="$HIVE_HOME/bin/hive -f $RUN_DIR/maas_report.hql -hiveconf start_date=$cdate                                                                             "
echo $cmd
$dry eval $cmd
cmd="$HIVE_HOME/bin/hive -f $RUN_DIR/maas_report1.hql -hiveconf start_date=$cdat                                                                             e"
echo $cmd
$dry eval $cmd
cdate=$(date -d "$cdate +1day" +%Y-%m-%d)
echo "new date"
echo $cdate
echo $cdate > $RUN_DIR/maas_date
rm $RUN_DIR/lock.file
