#!/bin/env bash

usage="$(basename "$0")"
if [[ $1 == "-h" ]]
then
  echo $usage
  echo "The script calls /data/acumen-admin/maas-etl/maas-notification-from-cf.sh to download files for specific dates"
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


cdate=$(awk '{print $1}' $RUN_DIR/notifications_date)
echo $cdate
currDate=$(date  +%Y-%m-%d)
echo $currDate
pDate=$(date -d "$currDate -1day" +%Y-%m-%d)

cmd="/data/acumen-admin/maas-etl/maas-notification-from-cf.sh $cdate $pDate"
echo $cmd
$dry eval $cmd
echo $currDate > $RUN_DIR/notifications_date
