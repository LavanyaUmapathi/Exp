USE maas;

DROP TABLE IF EXISTS alarms_stg;

CREATE TABLE alarms_stg (
id STRING,
label STRING,
entity_id STRING,
account_id STRING,
check_id STRING,
disabled BOOLEAN,
notification_plan_id STRING,
created_at BIGINT,
updated_at BIGINT,
metadata MAP<STRING,STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
COLLECTION ITEMS TERMINATED BY '|'
MAP KEYS TERMINATED BY '='
LINES TERMINATED BY '\n';

CREATE TABLE alarms (
id STRING,
label STRING,
entity_id STRING,
account_id STRING,
check_id STRING,
disabled BOOLEAN,
notification_plan_id STRING,
created_at BIGINT,
updated_at BIGINT,
metadata MAP<STRING,STRING>
)
STORED AS ORC;

LOAD DATA INPATH '/user/asilva/configs/alarm*' INTO TABLE alarms_stg;

--copy to ORC table
FROM alarms_stg stg
INSERT OVERWRITE TABLE alarms
 SELECT *;

DROP TABLE alarms_stg;
