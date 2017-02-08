USE stacktach;

DROP TABLE IF EXISTS events_stg;

CREATE TABLE events_stg (
context_request_id STRING,
context_quota_class STRING,
event_type STRING,
payload map<STRING,STRING>,
payload_metadata map<STRING,STRING>,
payload_bandwidth map<STRING,STRING>,
payload_image_meta map<STRING,STRING>,
priority STRING,
context_is_admin BOOLEAN,
context_ts BIGINT,
publisher_id STRING,
message_id STRING,
context_roles array<STRING>,
ts BIGINT,
month INT,
day INT,
year INT,
context_project_name STRING,
context_read_deleted BOOLEAN,
context_tenant STRING,
context_instance_lock_checked BOOLEAN,
context_project_id BIGINT,
context_user_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
COLLECTION ITEMS TERMINATED BY '|'
MAP KEYS TERMINATED BY '='
LINES TERMINATED BY '\n';

LOAD DATA INPATH ${IN_PATH} INTO TABLE events_stg;

CREATE TABLE events (
context_request_id STRING,
context_quota_class STRING,
event_type STRING,
payload map<STRING,STRING>,
payload_metadata map<STRING,STRING>,
payload_bandwidth map<STRING,STRING>,
payload_image_meta map<STRING,STRING>,
priority STRING,
context_is_admin BOOLEAN,
context_ts BIGINT,
publisher_id STRING,
message_id STRING,
context_roles array<STRING>,
ts BIGINT,
context_project_name STRING,
context_read_deleted BOOLEAN,
context_tenant STRING,
context_instance_lock_checked BOOLEAN,
context_project_id BIGINT,
context_user_name STRING
)
PARTITIONED BY (DT STRING)
STORED AS ORC;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.exec.compress.output=true;
SET hive.exec.max.dynamic.partitions=2000;
SET mapred.max.split.size=521000000;
SET mapred.output.compression.type=BLOCK;
SET io.sort.mb=256;
SET io.sort.factor=100;
SET mapred.job.reuse.jvm.num.tasks=-1;
SET hive.enforce.sorting=true;
SET mapreduce.reduce.input.limit = -1;
SET hive.merge.mapredfiles = true;
SET mapred.child.java.opts=-Xmx2048m;
SET mapred.child.ulimit=7316032;
SET mapred.job.reduce.memory.mb=4096;
SET mapred.job.map.memory.mb=4096;
SET mapred.task.maxvmem=9223372036854775807;

FROM events_stg stg
INSERT OVERWRITE TABLE events PARTITION(dt)
 SELECT
          stg.context_request_id,
          stg.context_quota_class,
          stg.event_type,
          payload,
          payload_metadata,
          payload_bandwidth,
          payload_image_meta,
          stg.priority,
          stg.context_is_admin,
          stg.context_ts,
          stg.publisher_id,
          stg.message_id,
          stg.context_roles,
          stg.ts,
          stg.context_project_name,
          stg.context_read_deleted,
          stg.context_tenant,
          stg.context_instance_lock_checked,
          stg.context_project_id,
          stg.context_user_name,
        CONCAT(stg.year,'-',
            CASE WHEN stg.month < 10 THEN concat('0',stg.month) ELSE trim(stg.month) END,'-',
            CASE WHEN stg.day < 10 THEN concat('0',stg.day) ELSE trim(stg.day) END) as m_date
            where stg.ts is not null
DISTRIBUTE BY m_date;

DROP TABLE events_stg;

