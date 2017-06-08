/home/airflow/airflow-jobs/scripts/ods_archiving/ods_incremental_partitioned_table_load.sh operational_reporting_avaya hvdn dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_avaya hvector dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_avaya mvdn dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_avaya d_secs dbo false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_avaya hcwc dbo false 
