/home/airflow/airflow-jobs/scripts/ods_archiving/ods_incremental_non_partitoned_table_load.sh rt_ods principals id dbo true false false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh rt_ods accountinfo dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh rt_ods queues dbo false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh rt_ods scripactions dbo false
