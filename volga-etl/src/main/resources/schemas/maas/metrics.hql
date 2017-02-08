USE maas;

DROP TABLE IF EXISTS metrics_stg;

CREATE TABLE metrics_stg (
id STRING,
account_id STRING,
tenant_id STRING,
entity_id STRING,
check_id STRING,
dimension_key STRING,
target STRING,
check_type STRING,
monitoring_zone_id STRING,
collector_id STRING,
available boolean,
ts BIGINT,
month INT,
day INT,
year INT,
metrics map<STRING,STRUCT<type:INT,value:STRING,unit:STRING,unit_other:STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
COLLECTION ITEMS TERMINATED BY '|'
MAP KEYS TERMINATED BY '='
LINES TERMINATED BY '\n';

LOAD DATA INPATH '/user/asilva/metrics/metric*' INTO TABLE metrics_stg;


CREATE TABLE metrics (
account_id STRING,
tenant_id STRING,
entity_id STRING,
check_id STRING,
target STRING,
monitoring_zone_id STRING,
collector_id STRING,
available boolean,
ts BIGINT,
check_type STRING,
metrics map<STRING,STRUCT<type:INT,value:STRING,unit:STRING,unit_other:STRING>>
)
PARTITIONED BY (DT STRING, HR STRING)
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

FROM metrics_stg stg
INSERT OVERWRITE TABLE metrics PARTITION(dt,hr)
 SELECT
          stg.account_id,
          stg.tenant_id,
          stg.entity_id,
          stg.check_id,
          stg.target,
          stg.monitoring_zone_id,
          stg.collector_id,
          stg.available,
          stg.ts,
          stg.check_type,
          stg.metrics,
        CONCAT(stg.year,'-',
            CASE WHEN stg.month < 10 THEN concat('0',stg.month) ELSE trim(stg.month) END,'-',
            CASE WHEN stg.day < 10 THEN concat('0',stg.day) ELSE trim(stg.day) END) as m_date,
            hour(from_unixtime(ts)) as m_hour
DISTRIBUTE BY m_date,m_hour;

DROP TABLE metrics_stg;


//EXPLODED VIEW

CREATE OR REPLACE VIEW vw_metrics AS
  SELECT
    account_id,
    tenant_id,
    entity_id,
    check_id,
    target,
    monitoring_zone_id,
    collector_id,
    available,
    ts,
    dt,
    check_type,
    metric_name,
    metric_value.type AS metric_type,
    metric_value.value AS metric_value,
    metric_value.unit AS metric_unit,
    metric_value.unit_other AS metric_unit_other
FROM metrics
LATERAL VIEW
  EXPLODE(metrics) ex_m AS metric_name, metric_value
  WHERE metrics IS NOT NULL
;
