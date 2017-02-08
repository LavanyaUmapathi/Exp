/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh lbaas_usage_events ah_updated dbo true
echo lbaas_usage_events backed up
echo backing up monitoring_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh monitoring_usage_events ah_updated dbo true
echo monitoring_usage_events backed up
echo backing up mysql_sites_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh mysql_sites_usage_events ah_updated dbo true
echo mysql_sites_usage_events backed up
echo backing up netapp_sites_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh netapp_sites_usage_events ah_updated dbo true
echo netapp_sites_usage_events backed up
echo backing up queues_bandwidth_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh queues_bandwidth_usage_events ah_updated dbo true
echo queues_bandwidth_usage_events backed up
echo backing up queues_queues_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh queues_queues_usage_events ah_updated dbo true
echo queues_queues_usage_events backed up
echo backing up servers_bandwidth_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh servers_bandwidth_usage_events ah_updated dbo true
echo servers_bandwidth_usage_events backed up
echo backing up servers_rhel_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh servers_rhel_usage_events ah_updated dbo true
echo servers_rhel_usage_events backed up
echo backing up servers_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh servers_usage_events ah_updated dbo true
echo servers_usage_events backed up
echo backing up sitessub_sites_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh sitessub_sites_usage_events ah_updated dbo true
echo sitessub_sites_usage_events backed up
echo backing up ssl_sites_usage_events
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh ssl_sites_usage_events ah_updated dbo true
echo ssl_sites_usage_events backed up
