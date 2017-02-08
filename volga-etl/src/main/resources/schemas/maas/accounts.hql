USE maas;

DROP TABLE IF EXISTS accounts_stg;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.optimize.bucketmapjoin=true;

CREATE TABLE accounts_stg (
id STRING,
external_id STRING,
min_check_interval INT,
soft_remove_ttl INT,
account_status STRING,
rackspace_managed BOOLEAN,
cep_group_id STRING,
agent_bundle_channel STRING,
check_type_channel STRING,
contact_ids ARRAY<STRING>,
entity_ids ARRAY<STRING>,
api_rate_limits MAP<STRING,INT>,
metrics_ttl MAP<STRING,INT>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
COLLECTION ITEMS TERMINATED BY '|'
MAP KEYS TERMINATED BY '='
LINES TERMINATED BY '\n';

CREATE TABLE accounts (
id STRING,
external_id STRING,
min_check_interval INT,
soft_remove_ttl INT,
account_status STRING,
rackspace_managed BOOLEAN,
cep_group_id STRING,
agent_bundle_channel STRING,
check_type_channel STRING,
contact_ids ARRAY<STRING>,
entity_ids ARRAY<STRING>,
api_rate_limits MAP<STRING,INT>,
metrics_ttl MAP<STRING,INT>
)
STORED AS ORC;

--load MR ouput
LOAD DATA INPATH '/user/asilva/configs/configuration*' INTO TABLE accounts_stg;

--copy to ORC table
FROM accounts_stg stg
INSERT OVERWRITE TABLE accounts
 SELECT *;


DROP TABLE accounts_stg;
