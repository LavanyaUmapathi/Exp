echo "Loading summary"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh summary created_date dbo false
echo "summary loaded"
echo "Loading summary_attr_value"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh summary_attr_value created_date dbo false
echo "summary_attr_value loaded"
echo "Loading  correlation"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh correlation created_date dbo false
echo "correlation loaded"
echo "Loading  details_correlation_xref"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh details_correlation_xref created_date dbo false
echo "details_correlation_xref loaded"
echo "Loading  usage_daily_daterange_xref"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh usage_daily_daterange_xref created_date dbo false
echo "usage_daily_daterange_xref loaded"
echo "Loading  usage_details"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh usage_details created_date dbo false
echo "usage_details loaded"
echo "Loading  usage_details_attribute_value"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh usage_details_attribute_value created_date dbo false
echo "usage_details loaded"
echo "Loading usage_summary"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh usage_summary created_date dbo false
echo "usage_summary loaded"
echo "Loading usage_summary_attribute_value"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh usage_summary_attribute_value created_date dbo false
echo "usage_summary_attribute_value loaded"