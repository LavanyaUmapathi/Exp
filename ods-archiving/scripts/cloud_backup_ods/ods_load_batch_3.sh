/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cloud_backup_ods backup_report_backup na dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cloud_backup_ods cleanup_report_error_list_core na dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cloud_backup_ods backup_configuration_backup na dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cloud_backup_ods restore_file_backup na dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods domain_username_reporting_frequency_core dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods authorization_feature_core dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods enum_clean_up_state_core dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods enum_machine_agent_flavor_core dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods enum_agent_tasks_core dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods enum_email_preference_backup dbo false
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods change_log_backup_item_backup dbo false
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods configuration_polling_core dbo false
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods website_setting_temp_core dbo false
