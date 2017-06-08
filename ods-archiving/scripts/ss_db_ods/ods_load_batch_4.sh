/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods tkt_merge_tickets dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods accounts_notes_all dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods tkt_merge_users dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods accounts_tags_all dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods teams_tags_all dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods users_tags_all dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods tkt_merge_category dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods sync_status dbo false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods phone_number_all dbo false 
