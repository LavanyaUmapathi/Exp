USE maas;

DROP TABLE IF EXISTS entities_stg;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.optimize.bucketmapjoin=true;

SET PARTITION_DATE;
SET hivevar:PARTITION_DATE;

CREATE TABLE entities_stg (
id STRING,
account_id STRING,
label STRING,
managed BOOLEAN,
uri STRING,
agent_id STRING,
created_at BIGINT,
updated_at BIGINT,
ip_addresses MAP<STRING,STRING>,
metadata MAP<STRING,STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
COLLECTION ITEMS TERMINATED BY '|'
MAP KEYS TERMINATED BY '='
LINES TERMINATED BY '\n';

CREATE TABLE entities (
id STRING,
account_id STRING,
label STRING,
managed BOOLEAN,
uri STRING,
agent_id STRING,
created_at BIGINT,
updated_at BIGINT,
ip_addresses MAP<STRING,STRING>,
metadata MAP<STRING,STRING>
)
STORED AS ORC;

LOAD DATA INPATH '/user/asilva/configs/entity*' INTO TABLE entities_stg;

--copy to ORC table
FROM entities_stg stg
INSERT OVERWRITE TABLE entities
SELECT *;

--Copy to history table
INSERT OVERWRITE TABLE entities_history
PARTITION (dt='${hivevar:PARTITION_DATE}')
SELECT *
FROM entities_stg;

DROP TABLE entities_stg;

