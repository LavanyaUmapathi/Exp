/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_avaya dtrunk dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_avaya haglog dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_avaya dbitems dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_avaya m_secs dbo false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_avaya vdnex dbo false 
