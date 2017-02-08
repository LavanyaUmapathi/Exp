echo backing up cloud_backup
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_backup dw_timestamp dbo false
echo cloud_backup backed up
echo backing up cloud_cbs
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_cbs dw_timestamp dbo false
echo cloud_cbs backed up
echo backing up cloud_dbaas
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_dbaas dw_timestamp dbo false
echo cloud_dbaas backed up
echo backing up cloud_files
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_files dw_timestamp dbo false
echo cloud_files backed up
echo backing up cloud_glance
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_glance dw_timestamp dbo false
echo cloud_glance backed up
echo backing up cloud_hadoop
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_hadoop dw_timestamp dbo false
echo cloud_hadoop backed up
echo backing up cloud_lbaas
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_lbaas dw_timestamp dbo false
echo cloud_lbaas backed up
echo backing up cloud_metered_sites
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_metered_sites dw_timestamp dbo false
echo cloud_metered_sites backed up
