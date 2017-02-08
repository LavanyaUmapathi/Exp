----Table definition for Stacktack requests. it has aggregated events data that captures details like statuses , request start time , its error messages , other image type and instance related attributes and details about 

USE neha8380;


CREATE TABLE

IF NOT EXISTS requests(hive_last_updated_ts TIMESTAMP, request_dt TIMESTAMP, request_id string, tenant_id string, instance_id string, request_start_ts TIMESTAMP, request_end_ts TIMESTAMP, instance_active_ts TIMESTAMP, has_end TINYINT, has_shutdown TINYINT, has_delete TINYINT, has_build TINYINT, cnt_error_events INT, cnt_error_states INT, cnt_error_recoveries INT, states STRING, task_states STRING, messages STRING, error_messages STRING, error_exception_codes STRING, first_state string, last_state string, first_state_description string, last_state_description string, first_task_state string, last_task_state string, cell string, region string, instance_flavor_id string, instance_type_name string, instance_type_flavorid string, instance_type_id string, instance_type_flavor_id string, os_type string, image_type string, memory_mb string, disk_gb string, root_gb string, ephemeral_gb string, vcpus string, instance_type string, rax_options string) partitioned BY (dt string) row format delimited fields terminated BY ',' escaped BY '\\' 
	COLLECTION items terminated BY '|' map keys terminated BY '=' lines terminated BY '\n' stored AS orc;
