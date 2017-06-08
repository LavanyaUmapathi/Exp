#!/bin/env bash

usage="$(basename "$0")"
if [[ $1 == "-h" ]]
then
  echo $usage
  echo "The script calls /data/acumen-admin/maas-etl/maas-metrics-from-cf.sh to download files for specific date"
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


cdate=$(awk '{print $1}' $RUN_DIR/maas_date)
echo $cdate
cmd="/data/acumen-admin/maas-etl/maas-metrics-from-cf.sh $cdate $cdate"
echo $cmd
$dry eval $cmd
#pDate=$(date -d "$cdate +1day" +%Y-%m-%d)
#cmd="echo $pDate > $RUN_DIR/maas_date"
#echo $cmd
#$dry eval $cmd
