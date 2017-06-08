#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_incremental_non_partitoned_table_load.sh operational_reporting_core computer_log modification_date CORE_Prod true true false
#/home/airflow/airflow-jobs/scripts/ods_archiving/get_last_records.sh operational_reporting_core computer_log id modification_date false false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core acct_xref_account_ticket_history dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core cont_contact dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core mntr_monitoredinterface dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core product_configuration dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core cntr_contract dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core acct_attribute_cache_dba_services dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core times_available dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core churn_forecast_service_names dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core cont_xref_contact_phonenumber_contactphonetype dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tckt_worktype dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core sku dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core qv_queueview_condition_field dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core sku_attributes dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tckt_queue dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core shared_storage_host_computer CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core qv_field_groups CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core calendar CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core upgd_queue CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tckt_val_statustype CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core product_category CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tckt_log_schedule CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core status_group CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core mntr_xref_sla_sku CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core acct_val_teamrole CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core revn_val_revenuedeletereason CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core mntr_val_alerttype CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core acct_xref_segment_contact_segmentmemberrole CORE_Prod false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core ntwk_switchmodelport CORE_Prod false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core upgrade_log CORE_Prod false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core vcc_xref_hypervisor_dropout LOGGING false 
