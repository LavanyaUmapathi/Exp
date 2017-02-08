#!/bin/bash
#
# Creates an airflow job for new ODS archive
#
# USAGE:  roll_out_ods.sh
# -n/--dryrun : show but do not execute
#
program=`basename $0`
#File System
airflow_jobs_home=/home/airflow/airflow-jobs
airflow_jobs_backup=/home/airflow/airflow-jobs-backup
current_date=$(date  +%s)
backup_dir=$airflow_jobs_backup/backup_$current_date
airflow_scripts_dir=$airflow_jobs_home/scripts
airflow_dag_dir=$airflow_jobs_home/dags
airflow_run_dir=/var/run/ods_archive
dags_sub_dir=dags
scripts_sub_dir=scripts
dates_sub_dir=dates
rollout_dir=/home/airflow/tmp/ods_archiving_rollout

dry=
replace_date=false
ods_name=
echo $program
echo $airflow_jobs_home
echo $airflow_jobs_backup
echo $current_date
echo $backup_dir
echo $airflow_scripts_dir
echo $airflow_dag_dir
echo $airflow_run_dir
echo $dags_sub_dir
echo $scripts_sub_dir
echo $dates_sub_dir

usage() {
  echo "Usage: $program [OPTIONS] [ODS_NAME] [REPLACE_DATE_FLAG]" 1>&2
  echo "OPTIONS:" 1>&2
  echo "  -h             Show this help message" 1>&2
  echo "  -n / --dryrun  Show but do not execute commands" 1>&2
}
while getopts "e:hn" o ; do
  case "${o}" in
    h)
     usage
     exit 0
     ;;
    n)
     dry=echo
     echo "Dry run will not execute"
     ;;
    \?)
      echo "$program: ERROR: Unknown option -$OPTARG" 1>&2
      echo "$program: Use $program -h for usage" 1>&2
      exit 1
      ;;
   esac
done
shift $((OPTIND-1))
echo $#
if [[ $# -gt 2 ]]; then
  echo "$program: ERROR: expected 0 1 or 2 argument" 1>&2
  usage
  exit 1
fi


ods_name=$1
replace_date=$2
echo $ods_name
echo $replace_date
if [[ ! -d $backup_dir ]]; then
   $dry mkdir $backup_dir
fi

$dry cp -r $airflow_jobs_home $backup_dir/.

if [[ -n $ods_name ]]; then
   echo "Promoting ODS: "$ods_name
   $dry cp -r $scripts_sub_dir/$ods_name $airflow_jobs_home/$scripts_sub_dir/$ods_name
   $dry cp $dags_sub_dir/$ods_name/$ods_name.py $airflow_jobs_home/$dags_sub_dir/.
   $dry python $airflow_jobs_home/$dags_sub_dir/$ods_name.py
   if [[ $replace_date == "true" ]]; then
       $dry cp $dates_sub_dir/$ods_name/* $airflow_run_dir/$ods_name
   fi
else
  echo "Promoting new ODS"
  ls $rollout_dir/$scripts_sub_dir/
#  exit
  for d in $rollout_dir/$scripts_sub_dir/*; do
         echo $d
     if [[ -d $d ]]; then
         echo "Process $d"
         ods_name=`basename $d`
         echo "ODS  name: $ods_name"
         if [[ ! -d $airflow_jobs_home/$scripts_sub_dir/$ods_name ]]; then
             echo "Promoting new ODS: "$ods_name
             $dry cp -r $rollout_dir/$scripts_sub_dir/$ods_name $airflow_jobs_home/$scripts_sub_dir/$ods_name
             $dry chmod 755 $airflow_jobs_home/$scripts_sub_dir/$ods_name/*.sh
             sudo -su ods_archive mkdir $airflow_run_dir/$ods_name
             sudo -su ods_archive cp $rollout_dir/processControlFlag $airflow_run_dir/$ods_name

             if [[ -d $rollout_dir/$dates_sub_dir/$ods_name ]]; then
                                echo "Creating run dates dir"
                                sudo -su ods_archive mkdir  $airflow_run_dir/$ods_name/$dates_sub_dir
                                sudo chown -R airflow:airflow $airflow_run_dir/$ods_name
                $dry cp $rollout_dir/$dates_sub_dir/$ods_name/$dates_sub_dir/* $airflow_run_dir/$ods_name/$dates_sub_dir

             fi
             sudo chown -R airflow:airflow $airflow_run_dir/$ods_name
             chmod 755 -R $airflow_run_dir/$ods_name
             $dry cp $rollout_dir/$dags_sub_dir/$ods_name.py $airflow_jobs_home/$dags_sub_dir/.
             source /usr/local/airflow-venv/bin/activate
             $dry python $airflow_jobs_home/$dags_sub_dir/$ods_name.py

         fi
     fi
  done
fi
