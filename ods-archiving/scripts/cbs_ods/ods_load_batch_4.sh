/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cbs_ods export_lunrdb dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cbs_ods services_cinderdb dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cbs_ods volume_type_lunrdb dbo false
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cbs_ods sm_backend_config_cinderdb dbo false
