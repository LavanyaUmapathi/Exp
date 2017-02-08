/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cloud_backup_ods cleanup_report_snapshot_core na dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cloud_backup_ods change_log_backup_configuration_backup na dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cloud_backup_ods schedule_backup na dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cloud_backup_ods restore_backup na dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods refresh_date_util dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods enum_backup_state_backup dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods enum_logging_level_core dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods enum_billing_triggers_billing dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods enum_image_support_status_core dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods enum_filter_backup dbo false
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods change_log_user_auth_domain_core dbo false
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cloud_backup_ods domain_username_meta_core dbo false
