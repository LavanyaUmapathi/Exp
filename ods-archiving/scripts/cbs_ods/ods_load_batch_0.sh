/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cbs_ods reservations_cinderdb dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cbs_ods snapshots_cinderdb dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_partitioned_table_load.sh cbs_ods node_lunrdb dw_timestamp dbo false true false
/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cbs_ods migrate_version_cinderdb dbo false
#/home/airflow/airflow-jobs/scripts/ods_archiving/ods_full_load.sh cbs_ods sm_flavors_cinderdb dbo false