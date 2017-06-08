/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods tkt_merge_comments dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods tkt_merge_tags dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods teams_accounts_flags_all dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods user_all dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods teams_users_roles_all dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods group_all dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods teams_internal_emails_all dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods teams_badges_all dbo false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods businessunits_segments_all dbo false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods users_badges_all dbo false 
