/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_avaya dsplit dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_avaya mtrunk dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_avaya mvector dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_avaya arch_stat dbo false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_avaya mcwc dbo false 
