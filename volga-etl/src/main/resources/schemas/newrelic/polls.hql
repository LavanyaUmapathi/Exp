create table polls (
	monitor_id string,
	poll_ts bigint,
	available boolean) 
	PARTITIONED BY (
	  dt string)
	ROW FORMAT SERDE
	  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
	STORED AS INPUTFORMAT
	  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
	OUTPUTFORMAT
	  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';