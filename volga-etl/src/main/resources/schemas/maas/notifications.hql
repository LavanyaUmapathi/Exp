USE maas;

DROP TABLE IF EXISTS notifications_stg;

CREATE TABLE notifications_stg (
event_id STRING,
log_entry_id STRING,
tenant_id STRING,
entity_id STRING,
check_id STRING,
check_type STRING,
alarm_id STRING,
target STRING,
ts BIGINT,
month INT,
day INT,
year INT,
status STRING,
state STRING,
metrics map<STRING,STRUCT<type:STRING,data:BIGINT,unit:STRING>>,
alarm_active_suppressions ARRAY<STRING>,
check_active_suppressions ARRAY<STRING>,
entity_active_suppressions ARRAY<STRING>,
is_suppressed BOOLEAN,
notifications ARRAY<STRING>,
observations ARRAY<STRUCT<monitoring_zone_id:STRING,state:STRING,status:STRING,`timestamp`:BIGINT,collector_state:STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
COLLECTION ITEMS TERMINATED BY '|'
MAP KEYS TERMINATED BY '='
LINES TERMINATED BY '\n';

CREATE TABLE notifications (
event_id STRING,
log_entry_id STRING,
tenant_id STRING,
entity_id STRING,
check_id STRING,
check_type STRING,
alarm_id STRING,
target STRING,
ts BIGINT,
status STRING,
state STRING,
metrics map<STRING,STRUCT<type:STRING,data:BIGINT,unit:STRING>>,
alarm_active_suppressions ARRAY<STRING>,
check_active_suppressions ARRAY<STRING>,
entity_active_suppressions ARRAY<STRING>,
is_suppressed BOOLEAN,
notifications ARRAY<STRING>,
observations ARRAY<STRUCT<monitoring_zone_id:STRING,state:STRING,status:STRING,`timestamp`:long,collector_state:STRING>>
)
PARTITIONED BY (DT STRING)
STORED AS ORC;

LOAD DATA INPATH '/user/asilva/notifications/notification*' INTO TABLE notifications_stg;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.optimize.bucketmapjoin=true;

FROM notifications_stg stg
INSERT OVERWRITE TABLE notifications PARTITION(DT)
   SELECT stg.event_id,
          stg.log_entry_id,
          stg.tenant_id,
          stg.entity_id,
          stg.check_id,
          stg.check_type,
          stg.alarm_id,
          stg.target,
          stg.ts,
          stg.status,
          stg.state,
          stg.metrics,
          stg.alarm_active_suppressions,
          stg.check_active_suppressions,
          stg.entity_active_suppressions,
          stg.is_suppressed,
          stg.notifications,
          stg.observations,
         CONCAT(stg.year,'-',
                     CASE WHEN stg.month < 10 THEN concat('0',stg.month) ELSE trim(cast(stg.month as string)) END,'-',
                     CASE WHEN stg.day < 10 THEN concat('0',stg.day) ELSE trim(cast(stg.day as string)) END) as m_date
DISTRIBUTE BY m_date;

DROP TABLE notifications_stg;