USE maas;

DROP TABLE IF EXISTS checks_stg;

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

DROP TABLE checks_stg;
