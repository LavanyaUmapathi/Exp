#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_incremental_non_partitoned_table_load.sh operational_reporting_core part_log modification_date CORE_Prod true true false
#/home/airflow/airflow-jobs/scripts/ods_archiving/get_last_records.sh operational_reporting_core part_log id modification_date false false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tckt_attribute_cache dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core build_tech dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core cntr_xref_contract_server dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tckt_rclog_ticket dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core cont_xref_contact_note dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core computer_xref_site_id dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tckt_val_subcategory dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core cont_phonenumber dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core server_custom_port dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tckt_val_status dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core escalator_logs dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core hypervisor_clusters dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core acct_products dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tckt_product dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core service_type CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core sub_product CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core crdt_val_feetype CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core product CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core mntr_val_servicepoller CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core ntwk_val_interfacetype CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core acct_val_accounttype CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core comp_val_osgroup CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core upgrade_status_options CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core comp_val_leasedlinemedium CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tckt_val_queuegroup CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core revn_val_revenuecategory CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core acct_val_products CORE_Prod false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core qv_queueview_permission CORE_Prod false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core upgrade_status_log CORE_Prod false 
