USE maas;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.optimize.bucketmapjoin=true; 

-- ACCOUNTS TABLE ----

DROP TABLE IF EXISTS accounts_history;

CREATE TABLE accounts_history (
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
PARTITIONED BY (DT STRING)
STORED AS ORC;

-- CHECKS TABLE -------

DROP TABLE IF EXISTS checks_history;

CREATE TABLE checks_history(
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
PARTITIONED BY (DT STRING)
STORED AS ORC;

-- ENTITIES TABLE -----------------

DROP TABLE IF EXISTS entities_history;

CREATE TABLE entities_history (
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
PARTITIONED BY (DT STRING)
STORED AS ORC;
