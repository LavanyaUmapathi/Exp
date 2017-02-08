use up_time_calculation;
insert overwrite table tmp_maas_configuration select distinct check_id,SUBSTR(monitoring_zone_id,3) from maas.metrics where dt='${hiveconf:start_date}';
SET hive.enforce.bucketing=true;
SET hive.auto.convert.join = false;
SET hive.execution.engine=mr;

insert overwrite table tmp_error_metrics_bucket select check_id, check_zone, ts, dt from
(select distinct check_id,SUBSTR(monitoring_zone_id,3) as check_zone,
 ts, dt from maas.metrics where dt='${hiveconf:start_date}' and available=false
 UNION ALL
 select a.check_id, a.check_monitoring_zone as check_zone, a.ts,'${hiveconf:start_date}' as dt from tmp_open_errors a JOIN maas.checks b
on a.check_id=b.id where b.disabled=false) m;


insert overwrite table tmp_good_metrics_bucket select  m.check_id, substr(m.monitoring_zone_id,3) , m.ts
 from (select check_id, monitoring_zone_id, ts from maas.metrics where dt = '${hiveconf:start_date}' and available =true) m
 JOIN (select distinct check_id from tmp_error_metrics_bucket) tmp on m.check_id=tmp.check_id;

 SET hive.execution.engine=mr;
 SET hive.enforce.bucketing=true;
 set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
 set hive.optimize.bucketmapjoin = true;
 set hive.optimize.bucketmapjoin.sortedmerge = true;

insert overwrite table up_time_calculation.tmp_error_metrics_duration
select  b.check_id, b.check_monitoring_zone, b.ts, min(g.ts) from
up_time_calculation.tmp_error_metrics_bucket b JOIN up_time_calculation.tmp_good_metrics_bucket g
on  b.check_id=g.check_id and
b.check_monitoring_zone=g.check_monitoring_zone
WHERE g.ts >= b.ts
group by b.check_id, b.check_monitoring_zone, b.ts;
