use up_time_calculation;

insert into table error_metrics_duration
select a.check_id, a.check_monitoring_zone ,
b.monitor_description as target, b.monitor_name as label, b.monitor_type, b.device_number, 'true', b.device_number,
b.device_number as device,
b.account_number,
a.start_time, a.end_time,'NewRelic' from (select check_id, check_monitoring_zone, min(start_time) as start_time, COALESCE(end_time, 0) as end_time
 from tmp_error_metrics_duration  where end_time !=0 group by check_id, check_monitoring_zone,end_time) a, new_relic.monitors b
where a.check_id = b.monitor_id and b.dt = '${hiveconf:start_date}';



insert into table up_time_calculation.tmp_error_metrics_duration select z.check_id, z.check_monitoring_zone, min(z.ts), z.ts_end
from  (select  b.check_id, b.check_monitoring_zone, b.ts, COALESCE(g.ts, 0) as ts_end
 from up_time_calculation.tmp_error_metrics_bucket b left outer join up_time_calculation.tmp_good_metrics_bucket g
 on (b.check_id=g.check_id )) z where z.ts_end=0
 group by z.check_id, z.check_monitoring_zone, z.ts_end;

insert overwrite table tmp_open_errors_new_relic
select check_id, check_monitoring_zone, start_time from tmp_error_metrics_duration where end_time=0;

insert into table open_errors
select a.check_id, a.check_monitoring_zone ,
b.monitor_description as target, b.monitor_name, b.monitor_type, b.device_number, 'true', b.device_number,
b.device_number as device,
b.account_number,
a.start_time,'NewRelic' from (select check_id, check_monitoring_zone, min(start_time) as start_time
 from tmp_error_metrics_duration where end_time=0 group by check_id, check_monitoring_zone,end_time) a, new_relic.monitors b
where a.check_id = b.monitor_id and b.dt = '${hiveconf:start_date}';
