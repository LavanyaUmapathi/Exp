/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cloud_backup_ods backup_backup na dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cloud_backup_ods clean_up_report_core na dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cloud_backup_ods backup_configuration_email_preference_backup na dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cloud_backup_ods email_recipient_backup na dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods backup_history_util dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods authorization_feature_group_core dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods enum_error_type_backup dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods enum_frequency_backup dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods change_type_core dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods enum_backup_type_backup dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods domain_core dbo false
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods configuration_heartbeat_core dbo false
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods machine_agent_min_disk_space_core dbo false