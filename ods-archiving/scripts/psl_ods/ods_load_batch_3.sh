/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh psl_ods method dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh psl_ods method_validation dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh psl_ods void_status dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh psl_ods status dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh psl_ods ach_payment_type dbo false 
