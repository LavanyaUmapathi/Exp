echo backing up backup_bandwidthin_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh backup_bandwidthin_usage_events ah_updated dbo true
echo backup_bandwidthin_usage_events backed up
echo backing up backup_bandwidthout_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh backup_bandwidthout_usage_events ah_updated dbo true
echo backup_bandwidthout_usage_events backed up
echo backing up backup_license_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh backup_license_usage_events ah_updated dbo true
echo backup_license_usage_events backed up
echo backing up backup_storage_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh backup_storage_usage_events ah_updated dbo true
echo backup_storage_usage_events backed up
echo backing up cbs_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cbs_usage_events ah_updated dbo true
echo cbs_usage_events backed up
echo backing up dbaas_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh dbaas_usage_events ah_updated dbo true
echo dbaas_usage_events backed up
echo backing up files_bandwidth_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh files_bandwidth_usage_events ah_updated dbo true
echo files_bandwidth_usage_events backed up
echo backing up files_cdnbandwidth_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh files_cdnbandwidth_usage_events ah_updated dbo true
echo files_cdnbandwidth_usage_events backed up
echo backing up files_storage_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh files_storage_usage_events ah_updated dbo true
echo files_storage_usage_events backed up
echo backing up hadoop_hdp1_3_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh hadoop_hdp1_3_usage_events ah_updated dbo true
echo hadoop_hdp1_3_usage_events backed up
