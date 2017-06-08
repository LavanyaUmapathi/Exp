USE maas;

DROP TABLE IF EXISTS checks_stg;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.optimize.bucketmapjoin=true;

SET PARTITION_DATE;
SET hivevar:PARTITION_DATE;

CREATE TABLE checks_stg (
id STRING,
entity_id STRING,
account_id STRING,
label STRING,
type STRING,
timeout INT,
period INT,
target_alias STRING,
target_hostname STRING,
target_resolver STRING,
disabled BOOLEAN,
created_at BIGINT,
updated_at BIGINT,
details MAP<STRING,STRING>,
monitoring_zones_poll ARRAY<STRING>,
metadata MAP<STRING,STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
COLLECTION ITEMS TERMINATED BY '|'
MAP KEYS TERMINATED BY '='
LINES TERMINATED BY '\n';


CREATE TABLE checks(
id STRING,
entity_id STRING,
account_id STRING,
label STRING,
type STRING,
timeout INT,
period INT,
target_alias STRING,
target_hostname STRING,
target_resolver STRING,
disabled BOOLEAN,
created_at BIGINT,
updated_at BIGINT,
details MAP<STRING,STRING>,
monitoring_zones_poll ARRAY<STRING>,
metadata MAP<STRING,STRING>
)
STORED AS ORC;

LOAD DATA INPATH '/user/asilva/configs/check*' INTO TABLE checks_stg;

--copy to ORC table
FROM checks_stg stg
INSERT OVERWRITE TABLE checks
SELECT *;

--copy to history table
INSERT OVERWRITE TABLE checks_history
PARTITION (dt='${hivevar:PARTITION_DATE}')
SELECT *
FROM checks_stg;

DROP TABLE checks_stg;

