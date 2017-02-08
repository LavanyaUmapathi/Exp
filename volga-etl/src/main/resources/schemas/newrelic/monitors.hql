create table monitors (
	monitor_id string,
	monitor_status string,
	monitor_name string,
	created_date bigint,
	modified_date bigint,
	frequency int,
	location string,
	device_number string,
	device_source_system string,
	account_number string,
	account_source_system string,
	monitor_type string,
	monitor_description string)
	PARTITIONED BY (
	  dt string)
	ROW FORMAT SERDE
	  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
	STORED AS INPUTFORMAT
	  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
	OUTPUTFORMAT
	  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';