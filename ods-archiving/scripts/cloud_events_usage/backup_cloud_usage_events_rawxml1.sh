cd /home/airflow/airflow-jobs/scripts/cloud_events_usage
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_monitoring dw_timestamp dbo false
echo cloud_monitoring backed up
echo backing up cloud_mssql_sites
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_mssql_sites dw_timestamp dbo false
echo cloud_mssql_sites backed up
echo backing up cloud_mysql_sites
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_mysql_sites dw_timestamp dbo false
echo cloud_mysql_sites backed up
echo backing up cloud_netapp_sites
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_netapp_sites dw_timestamp dbo false
echo cloud_netapp_sites backed up
echo backing up cloud_queues
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_queues dw_timestamp dbo false
echo cloud_queues backed up
echo backing up cloud_servers
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_servers dw_timestamp dbo false
echo cloud_servers backed up
echo backing up cloud_sitessub_sites
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_sitessub_sites dw_timestamp dbo false
echo cloud_sitessub_sites backed up
echo backing up cloud_ssl_sites
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_ssl_sites dw_timestamp dbo false
echo cloud_ssl_sites backed up
echo backing up cloud_nova
/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup.sh cloud_nova dw_timestamp dbo false
echo cloud_nova backed up
