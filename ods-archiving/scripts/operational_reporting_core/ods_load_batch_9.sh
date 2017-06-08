#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_incremental_non_partitoned_table_load.sh operational_reporting_core server_parts modification_date CORE_Prod true true false
#/home/airflow/airflow-jobs/scripts/ods_archiving/get_last_records.sh operational_reporting_core server_parts server_parts_id modification_date false false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tckt_log_work dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core server dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core queue_cancel_server dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core crdt_creditmemolog dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core cert_xref_comp_cert dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core xref_customer_number_account dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core acct_attribute_cache dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core acct_accountprofitreport dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core employee_authorization dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core server_xref_custom_monitor dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core mntr_contactemail dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core vcc_xref_hypervisor_cluster dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core product_os dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core offline_reasons CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core escalator_rules CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core cont_department CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core datacenter CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tckt_log_sphereofsupport CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core qv_permission_group CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tckt_val_workunit CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core comp_val_leasedlinecarrier CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core qv_permission CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core crdt_val_currencytype CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core mntr_val_alertstatus CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core churn_forecast_status CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core acct_val_accountroletype CORE_Prod false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core escalator_error_logs CORE_Prod false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tckt_dependency CORE_Prod false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core hypervisor_clusters_xref_virtual_machines_dropout LOGGING false 
