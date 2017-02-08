USE neha8380;

DROP TABLE IF EXISTS events;
CREATE TABLE IF NOT EXISTS events 
	(	file STRING
	,	region STRING
	,	cell STRING
	,	dt_ts_utc STRING
	,	dt_utc STRING
	,	notification_timestamp STRING
	,	event_type	STRING               
	,	tenant_id   STRING                         	
	,	user_id  STRING                          	
	,	request_id  STRING        	
	,	message_id  STRING 
	,	message  STRING                                        	
	,	instance_id  STRING      	
	,	host  STRING             	
	,	service  STRING          	
	,	instance_flavor  STRING                          	
	,	instance_flavor_id  STRING	
	,	instance_type_name STRING
	,	instance_type_flavorid STRING
	,	instance_type_id STRING
	,	instance_type_flavor_id STRING
	, 	os_type STRING
	,	image_type STRING
	,	memory_mb  INT                            	
	,	disk_gb   INT         	
	,	root_gb   INT         	
	,	ephemeral_gb  INT     	
	,	vcpus  INT            	
	,	instance_type  STRING    	
	,	state  STRING            	
	,	old_state  STRING
	,	state_description    STRING            	
	,	os_architecture  STRING                           	
	,	os_version  STRING        	
	,	os_distro  STRING        	
	,	rax_options  STRING       	
	,	launched_at_utc STRING       	
	,	deleted_at_utc   STRING                                            	
	,	display_name  STRING     	
	,	audit_period_beginning_utc    STRING     	
	,	audit_period_ending_utc  STRING
	,	exception_code STRING
	,	terminated_at_utc  STRING 
	,   priority STRING 
	,	publisher_id STRING
	,	cell_name  STRING
	,	new_task_state STRING
	,	old_task_state STRING
	,	progress STRING
	,	image_ref_url STRING 
	)
PARTITIONED BY (record_received_dt_utc STRING)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	ESCAPED BY '\\'
	COLLECTION ITEMS TERMINATED BY '|'
	MAP KEYS TERMINATED BY '='
	LINES TERMINATED BY '\n'
STORED AS ORC ;