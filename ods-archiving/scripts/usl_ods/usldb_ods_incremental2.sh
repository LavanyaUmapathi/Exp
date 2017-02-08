echo "Loading usg_billing_details"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh usg_billing_details created_date dbo false
echo "usg_billing_details loaded"
echo "Loading usg_bill_dtls_attr_value"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh usg_bill_dtls_attr_value created_date dbo false
echo "usg_bill_dtls_attr_value loaded"
echo "Loading usg_bill_dtls_daily_xref"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh usg_bill_dtls_daily_xref created_date dbo false
echo "usg_bill_dtls_daily_xref loaded"
echo "Loading usg_billing_summary"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh usg_billing_summary created_date dbo false
echo "usg_billing_summary loaded"
echo "Loading usg_details_summary_xref"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh usg_details_summary_xref created_date dbo false
echo "usg_details_summary_xref loaded"
echo "Loading usg_error_details"
/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_backup_incremental.sh usg_error_details created_date dbo false
echo "usg_error_details loaded"
