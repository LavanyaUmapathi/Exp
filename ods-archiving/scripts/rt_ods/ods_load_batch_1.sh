/home/airflow/airflow-jobs/scripts/ods_archiving/ods_incremental_non_partitoned_table_load.sh rt_ods cachedgroupmembers id dbo true false false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_incremental_partitioned_table_load.sh rt_ods objectcustomfieldvalues lastupdated dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh rt_ods attributes dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh rt_ods links dbo false
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh rt_ods sessions dbo false
