/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods tkt_merge_tickettags dbo true 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods tkt_merge_products dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods notes_categories_all dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods groups_users_all dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods account_plan_request_all dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods teams_chatteams_all dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods tkt_merge_sub_category dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods badge_all dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods note_category_all dbo false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods service_levels_all dbo false 
