#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_incremental_non_partitoned_table_load.sh operational_reporting_core tckt_log_assignment modification_date CORE_Prod true true false
#/home/airflow/airflow-jobs/scripts/ods_archiving/get_last_records.sh operational_reporting_core tckt_log_assignment tckt_log_assignment_slony_id modification_date false false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core parts_attributes dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core sales_speed dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core churn_forecast_xref_server dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tckt_xref_subticket dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core addr_privatenetip dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core hypervisors_xref_virtual_machines dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core revn_xref_revenue_server dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core crdt_xref_creditmemo_department dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core cont_xref_contact_email_contactemailtype dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tsur_ticketsurvey dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tckt_xref_queue_category dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core skunit dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core escalator_rule_conditions dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core escalator_rule_actions CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core acct_val_onyxcountrycode CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tckt_action CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core status_options CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core mntr_xref_sla_servicepoller CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core crdt_val_creditmemologtype CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core mntr_netsaintpollmethod CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core qv_field_type CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core revn_val_revenuestatus CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core acct_val_servicelevel CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core cont_val_contactemailtype CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core acct_region CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tckt_val_approvalstatus CORE_Prod false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core acct_account_bkp CORE_Prod false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tckt_column CORE_Prod false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core xref_sku_skugroup CORE_Prod false 
