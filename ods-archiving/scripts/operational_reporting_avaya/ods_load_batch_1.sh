/home/airflow/airflow-jobs/scripts/ods_archiving/ods_incremental_partitioned_table_load.sh operational_reporting_avaya hsplit dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_avaya dvector dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_avaya htkgrp dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_avaya customer_log dbo false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_avaya f_cdayrep dbo false 
