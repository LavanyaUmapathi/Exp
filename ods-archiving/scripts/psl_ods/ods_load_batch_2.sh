/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh psl_ods payment dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh psl_ods account dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh psl_ods void dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh psl_ods payment_service_client dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh psl_ods method_type dbo false 
