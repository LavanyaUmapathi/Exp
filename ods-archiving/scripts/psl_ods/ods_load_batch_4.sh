/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh psl_ods payment_card dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh psl_ods avs_information dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh psl_ods electronic_check dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh psl_ods payment_card_type dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh psl_ods region dbo false 
