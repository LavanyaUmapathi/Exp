use up_time_calculation;
SET hive.auto.convert.join = false;
SET hive.execution.engine=mr;

insert into table error_metrics_duration
select a.check_id, a.check_monitoring_zone ,
case when ch.details['url'] is not NULL Then ch.details['url']
when ch.details['target'] is not NULL Then ch.details['target']
else NULL End as target, ch.label, ch.type, en.label, en.managed, en.uri,
reverse(split(reverse(en.uri),'/')[0]) as device,
ac.external_id,
a.start_time, a.end_time,'MaaS' as source_system_name from (select check_id, check_monitoring_zone, min(start_time) as start_time, COALESCE(end_time, 0) as end_time
 from tmp_error_metrics_duration  where end_time !=0 group by check_id, check_monitoring_zone,end_time) a, maas.checks ch, maas.entities  en , maas.accounts ac
where a.check_id = ch.id and ch.entity_id=en.id and ch.account_id=ac.id;



insert into table up_time_calculation.tmp_error_metrics_duration select z.check_id, z.check_monitoring_zone, min(z.ts), z.ts_end
from  (select  b.check_id, b.check_monitoring_zone, b.ts, COALESCE(g.ts, 0) as ts_end
 from up_time_calculation.tmp_error_metrics_bucket b left outer join up_time_calculation.tmp_good_metrics_bucket g
 on (b.check_id=g.check_id and b.check_monitoring_zone=g.check_monitoring_zone)) z where z.ts_end=0
 group by z.check_id, z.check_monitoring_zone, z.ts_end;

insert overwrite table tmp_open_errors
select check_id, check_monitoring_zone, start_time from tmp_error_metrics_duration where end_time=0;

insert overwrite table open_errors
select a.check_id, a.check_monitoring_zone ,
case when ch.details['url'] is not NULL Then ch.details['url']
when ch.details['target'] is not NULL Then ch.details['target']
else NULL End as target, ch.label, ch.type, en.label, en.managed, en.uri,
reverse(split(reverse(en.uri),'/')[0]) as device,
ac.external_id,
a.start_time, 'MaaS' as source_system_name from (select check_id, check_monitoring_zone, min(start_time) as start_time
 from tmp_error_metrics_duration where end_time=0 group by check_id, check_monitoring_zone,end_time) a, maas.checks ch, maas.entities  en , maas.accounts ac
where a.check_id = ch.id and ch.entity_id=en.id and ch.account_id=ac.id;
