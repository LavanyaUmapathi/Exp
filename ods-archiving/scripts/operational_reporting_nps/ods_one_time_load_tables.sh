/home/airflow/airflow-jobs/scripts/ods_archiving/ods_incremental_non_partitoned_table_load.sh operational_reporting_nps ckbx_responseanswers dw_timestamp dbo true false false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_nps ckbx_response dbo true 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_nps ckbx_item dbo false 
