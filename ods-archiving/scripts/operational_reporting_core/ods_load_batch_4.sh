#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_incremental_non_partitoned_table_load.sh operational_reporting_core tckt_log_ticketstate tckt_log_ticketstateid CORE_Prod true false false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tckt_ticket dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core ipsp_cache_ipassignment dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core mntr_monitorednode dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core addr_natcomment dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core churn_forecasts dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core upgd_scheduledservice dw_timestamp CORE_Prod true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core ntwk_switch dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core sku_attribute_to_sku dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core vcc_xref_hypervisor dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tckt_massticketjob dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core hypervisor_clusters_xref_hypervisors dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core tckt_rclog_admin dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core upgd_val_servicetype dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh operational_reporting_core future_ticket dw_timestamp CORE_Prod false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core xref_account_opt_out CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core status_xref_status_group CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tckt_xref_category_message CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core qv_permission_group_field CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core qv_condition CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core crdt_val_servicefailuregroup CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tsur_ratingreason CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tckt_val_closeattr CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tckt_val_queuerole CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core cert_val_ordertype CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core tckt_val_priority CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core mntr_val_sla CORE_Prod false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core atom_hopper_markers LOGGING false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core ntwk_xref_port_computer_interfacetype CORE_Prod false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh operational_reporting_core upgrade_request CORE_Prod false 
