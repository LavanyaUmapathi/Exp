use up_time_calculation;
 SET hive.enforce.bucketing=true;
insert overwrite table tmp_error_metrics_bucket
select a.monitor_id,a.location, b.poll_ts,b.dt from
new_relic.monitors a JOIN new_relic.polls b on a.monitor_id = b.monitor_id   where a.dt='${hiveconf:start_date}' and b.dt='${hiveconf:start_date}'
 and b.available=false
 UNION ALL
 select a.check_id, a.check_monitoring_zone as check_zone, a.ts,'${hiveconf:start_date}' as dt from tmp_open_errors_new_relic a JOIN new_relic.monitors b
on a.check_id=b.monitor_id where b.dt='${hiveconf:start_date}';


insert overwrite table tmp_good_metrics_bucket select  m.monitor_id, m.location , m.ts
 from (select a.monitor_id,b.location, a.poll_ts as ts from new_relic.polls a JOIN new_relic.monitors b on a.monitor_id=b.monitor_id   where a.dt = '${hiveconf:start_date}' and b.dt = '${hiveconf:start_date}' and a.available =true) m
 JOIN (select distinct check_id from tmp_error_metrics_bucket) tmp on m.monitor_id=tmp.check_id;

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
