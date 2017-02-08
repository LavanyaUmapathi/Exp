----Table definition for stacktack requests derived from aggregated events data using stacktach.events dataset---- 
----This ETL is set to retrieve requests from events received from stacktach in past 15 days , so everyday past 15 days partitions are reloaded ----- 
----Fields used to derive % build success rate are [has_build,has_end]-----
----Fields used to analyse failed or  not failed but not completed builds [has_build,has_end,has_shutdown,has_delete,cnt_error*,error_messages]
----Fields used to analyse lifecycle of any instance are [instance_id,request_id,states,task_states,request_start_ts,request_end_ts ]
----Fields used to derive avg duration for successful builds requests are [has_build,has_end,request_end_ts - request_start_ts]
----Fields used to derive avg duration for the 1st occurrence of an instance in a day to land in an active state are [instance_id , dt ,has_build ,min(instance_active_ts)- min(request_start_ts) ]
----All other textual fields can be used for aggregation and summary. 

USE neha8380; -- in production -- USE stacktach; 

CREATE TABLE if not exists requests (
	hive_last_updated_ts TIMESTAMP,
	request_dt STRING,
	request_id STRING,
	tenant_id STRING,
	instance_id STRING,
	request_start_ts TIMESTAMP,
	request_end_ts TIMESTAMP,
	instance_active_ts TIMESTAMP,
	has_end TINYINT,
	has_shutdown TINYINT,
	has_delete TINYINT,
	has_build TINYINT,
	cnt_error_events INT,
	cnt_error_states INT,
	cnt_error_recoveries INT,
	states STRING,
	task_states STRING,
	messages STRING,
	error_messages STRING,
	error_exception_codes STRING,
	event_types STRING,
	first_state STRING,
	last_state STRING,
	first_state_description STRING,
	last_state_description STRING,
	first_task_state STRING,
	last_task_state STRING,
	cell STRING,
	region STRING,
	instance_flavor_id STRING,
	instance_type_name STRING,
	instance_type_flavorid STRING,
	instance_type_id STRING,
	instance_type_flavor_id STRING,
	os_type STRING,
	image_type STRING,
	memory_mb STRING,
	disk_gb STRING,
	root_gb STRING,
	ephemeral_gb STRING,
	vcpus STRING,
	instance_type STRING,
	rax_options STRING
	) partitioned BY (dt STRING) row format delimited fields terminated BY ',' escaped BY '\\' COLLECTION items terminated BY '|' map keys terminated BY '=' lines terminated BY '\n' stored AS orc;

DROP TABLE IF EXISTS temp_stg_requests; -- in production this name needs to change to temporary table name variable with process id--

CREATE TABLE temp_stg_requests AS
SELECT from_unixtime(unix_timestamp()) hive_last_updated_ts,
	min(dt) dt,
	min(to_date(cast(events.dt_ts_utc AS TIMESTAMP))) request_dt,
	events.request_id,
	events.tenant_id,
	events.instance_id,
	min(events.dt_ts_utc) request_start_ts,
	max(events.dt_ts_utc) request_end_ts,
	max(CASE 
			WHEN events.old_state != 'active'
				AND events.STATE = 'active'
				THEN dt_ts_utc
			END) instance_active_ts,
	max(CASE 
			WHEN events.event_type = 'compute.instance.create.end'
				THEN 1
			ELSE 0
			END) has_end,
	max(CASE 
			WHEN events.event_type LIKE '%shutdown%'
				THEN 1
			ELSE 0
			END) has_shutdown,
	max(CASE 
			WHEN events.event_type LIKE '%delete%'
				THEN 1
			ELSE 0
			END) has_delete,
	max(CASE 
			WHEN events.event_type = 'compute.instance.update'
				AND events.host LIKE '%nova-api%'
				AND events.STATE = 'building'
				AND events.state_description = 'scheduling'
				THEN 1
			ELSE 0
			END) has_build,
	sum(CASE 
			WHEN events.event_type LIKE '%error%'
				THEN 1
			ELSE 0
			END) cnt_error_events,
	sum(CASE 
			WHEN events.STATE LIKE '%error%'
				AND events.old_state NOT LIKE 'error%'
				THEN 1
			ELSE 0
			END) cnt_error_states,
	sum(CASE 
			WHEN events.STATE NOT LIKE '%error%'
				AND events.old_state LIKE 'error%'
				THEN 1
			ELSE 0
			END) cnt_error_recoveries,
	concat_ws('|', collect_set(events.STATE)) states,
	concat_ws('|', collect_set(events.new_task_state)) task_states,
	concat_ws('|', collect_set(events.message)) messages,
	concat_ws('|', collect_set(CASE 
				WHEN events.event_type LIKE '%error%'
					THEN events.message
				ELSE NULL
				END)) error_messages,
	concat_ws('|', collect_set(CASE 
				WHEN events.event_type LIKE '%error%'
					THEN events.exception_code
				ELSE NULL
				END)) error_exception_codes,
	concat_ws('|', collect_set(events.event_type)) event_types,
	max(CASE 
			WHEN event_rank_asc = 1
				THEN events.STATE
			ELSE NULL
			END) first_state,
	max(CASE 
			WHEN event_rank_desc = 1
				THEN events.STATE
			ELSE NULL
			END) last_state,
	max(CASE 
			WHEN event_rank_asc = 1
				THEN events.state_description
			ELSE NULL
			END) first_state_description,
	max(CASE 
			WHEN event_rank_desc = 1
				THEN events.state_description
			ELSE NULL
			END) last_state_description,
	max(CASE 
			WHEN event_rank_asc = 1
				THEN events.new_task_state
			ELSE NULL
			END) first_task_state,
	max(CASE 
			WHEN event_rank_desc = 1
				THEN events.new_task_state
			ELSE NULL
			END) last_task_state,
	max(events.cell) cell,
	max(events.region) region,
	max(events.instance_flavor_id) instance_flavor_id,
	max(events.instance_type_name) instance_type_name,
	max(events.instance_type_flavorid) instance_type_flavorid,
	max(events.instance_type_id) instance_type_id,
	max(events.instance_type_flavor_id) instance_type_flavor_id,
	max(events.os_type) os_type,
	max(events.image_type) image_type,
	max(events.memory_mb) memory_mb,
	max(events.disk_gb) disk_gb,
	max(events.root_gb) root_gb,
	max(events.ephemeral_gb) ephemeral_gb,
	max(events.vcpus) vcpus,
	max(events.instance_type) instance_type,
	max(events.rax_options) rax_options
--cast( round(cast((cast(cast(last_ts as timestamp) as double )- cast (cast(first_ts as timestamp) as double )) as double)*1000) as bigint ) 
FROM (
	SELECT e.*,
		row_number() OVER (
			PARTITION BY e.tenant_id,
			e.instance_id,
			e.request_id ORDER BY e.dt_ts_utc ASC
			) event_rank_asc,
		row_number() OVER (
			PARTITION BY e.request_id,
			e.instance_id,
			e.tenant_id ORDER BY dt_ts_utc DESC
			) event_rank_desc
	FROM stacktach.events e
	WHERE
		-- replace below with date parameter 'dt between rundate - 15 and rundate - 1' inclusive 		
		dt LIKE '2015-06-%'
	SORT BY 
		e.tenant_id,
		e.instance_id,
		e.request_id,
		e.dt_ts_utc
	) events
GROUP BY 
	events.request_id,
	events.instance_id,
	events.tenant_id;

SET hive.EXEC.DYNAMIC.PARTITION = true;
SET hive.EXEC.DYNAMIC.PARTITION.mode = nonstrict;

INSERT overwrite TABLE requests PARTITION (dt)
SELECT hive_last_updated_ts,
	request_dt,
	request_id,
	tenant_id,
	instance_id,
	request_start_ts,
	request_end_ts,
	instance_active_ts,
	has_end,
	has_shutdown,
	has_delete,
	has_build,
	cnt_error_events,
	cnt_error_states,
	cnt_error_recoveries,
	states,
	task_states,
	messages,
	error_messages,
	error_exception_codes,
	event_types,
	first_state,
	last_state,
	first_state_description,
	last_state_description,
	first_task_state,
	last_task_state,
	cell,
	region,
	instance_flavor_id,
	instance_type_name,
	instance_type_flavorid,
	instance_type_id,
	instance_type_flavor_id,
	os_type,
	image_type,
	memory_mb,
	disk_gb,
	root_gb,
	ephemeral_gb,
	vcpus,
	instance_type,
	rax_options,
	dt
FROM temp_stg_requests;

DROP TABLE IF EXISTS temp_stg_requests;
