/home/airflow/airflow-jobs/scripts/ods_archiving/ods_incremental_non_partitoned_table_load.sh ss_db_ods tkt_merge_value_changes dw_timestamp dbo true false false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods teams_accounts_all dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods note_all dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods account_all dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods instruction_all dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods tkt_merge_groups dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods phoneteams_numbers_all dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods chat_team_all dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods role_all dbo false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods product_all dbo false 
