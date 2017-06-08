/home/airflow/airflow-jobs/scripts/ods_archiving/ods_incremental_non_partitoned_table_load.sh ss_db_ods tkt_merge_events created_at dbo true false false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods tkt_merge_tickets_bu20140507 dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods accounts_users_roles_all dw_timestamp dbo true true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods error dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh ss_db_ods accounts_badges_all dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods team_all dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods teams_internal_phone_numbers_all dbo false 
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods phone_team_all dbo false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods accounts_contacts_all dbo false 
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh ss_db_ods teams_phoneteams_all dbo false 
