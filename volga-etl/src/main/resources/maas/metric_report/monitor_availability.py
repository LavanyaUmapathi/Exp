from pyspark import SparkConf, SparkContext, SQLContext

from pyspark.sql import HiveContext
from pyspark.sql.types import *
import constants as c
import time
import datetime
import os
from dateutil.relativedelta import relativedelta
from operator import itemgetter
#conf = SparkConf().setAppName("Monitor DownTime Calculation")
#conf.set("spark.driver.maxResultSize", "5g")
#sc = SparkContext(conf = conf)
#sqlContext= HiveContext(sc)

suppressionMapBC={}
deviceMonitorMapBC={}
ts_BC=()
monitorCreatedBC={}
number_of_seconds_in_month_BC=0


def calculatemonitoruptime(et):
    monitor=et[0][0]
    monitor_zone=et[0][1]
    downtime=0
    downtime_with_suppression=0
    rset=None
    device=None
    dev_supressions=None
    try:
        device=deviceMonitorMapBC.value[monitor]
        try:
                dev_supressions = suppressionMapBC.value[device]
        except:
                dev_supression=None

    except:
        dev_supressions=None


    for one_downtime in et[1]:
        error_start=one_downtime[0]
        error_end=one_downtime[1]
        if error_start<ts_BC.value[0]:
           error_start=ts_BC.value[0]
        if error_end>ts_BC.value[1]:
           error_end=ts_BC.value[1]


        error_duration=error_end-error_start
        error_duration_with_suppression=error_duration
        if dev_supressions != None:

            for one_supression in dev_supressions:
                s_start = one_supression[0]*1000
                s_end=one_supression[1]*1000
                if s_start <= error_start and s_end >= error_start:
                        if s_end >= error_end:
                                error_duration_with_suppression=0
                                break;
                        else:
                                error_start=s_end
                                error_duration_with_suppression=error_end-error_start
                                break
                else:
                        if s_start > error_start and s_end < error_end:
                                error_duration_with_suppression = s_end-s_start
                                break
                        else:
                                if s_start > error_start and s_start < error_end and s_end > error_end:
                                        error_duration_with_suppression=s_start-error_start
                                        break

        downtime=downtime+error_duration
        downtime_with_suppression=downtime_with_suppression+error_duration_with_suppression
    created_ts=0
    try:
        created_ts = monitorCreatedBC.value[monitor]
    except:
        return(monitor, monitor_zone, downtime,downtime_with_suppression, 0, '{0:.6f}'.format(0),'{0:.6f}'.format(0))


    up_time=0;
    if created_ts < ts_BC.value[0]:
        up_time=number_of_seconds_in_month_BC.value
    else:
        up_time=ts_BC.value[1]-created_ts
    suppression_diff=downtime-downtime_with_suppression
    if up_time == 0:
        return(monitor, monitor_zone, downtime,downtime_with_suppression, up_time,'{0:.6f}'.format(1), '{0:.6f}'.format(1) )
    up_time_percent=float(1) - float(downtime)/up_time
    agreed_uptime=up_time-suppression_diff
    if agreed_uptime == 0:
        #Aggreed time is 0 so avalibility is 1
        return(monitor, monitor_zone, downtime,downtime_with_suppression, up_time,'{0:.6f}'.format(up_time_percent), '{0:.6f}'.format(1) )
    up_time_with_suppression_percent=float(1) - float(downtime_with_suppression)/float(agreed_uptime)
    return(monitor,monitor_zone, downtime,downtime_with_suppression, up_time,'{0:.6f}'.format(up_time_percent), '{0:.6f}'.format(up_time_with_suppression_percent) )
run_date=''
with open (c.MONITOR_DATE_MARKER_FILE, 'r') as date_file:
    run_date = date_file.read()
print run_date
start_date=datetime.datetime.strptime(run_date.strip(), '%Y-%m-%d')
print start_date
start_ts = int(datetime.datetime.strptime(run_date.strip(), '%Y-%m-%d').strftime("%s"))*1000
end_date = start_date+ relativedelta(months=+1)
print end_date
end_ts =int(end_date.strftime("%s"))*1000
print end_ts
print start_ts
suppression_start_ts = int(datetime.datetime.strptime(run_date.strip(), '%Y-%m-%d').strftime("%s"))
supression_end_ts =  int(end_date.strftime("%s"))
print 'Supression'
print suppression_start_ts
print supression_end_ts

get_monitor_data="select a.check_id,a.check_monitoring_zone, a.start_time,a.end_time from (select check_id ,check_monitoring_zone, start_time, end_time from "\
                 "up_time_calculation.error_metrics_duration where (start_time >= {0} and start_time <{1}) or (start_time < {2} and end_time > {3}) UNION ALL "\
                 "select check_id ,check_monitoring_zone, start_time, 7227103875000 from up_time_calculation.open_errors where start_time < {4} ) a "\
                 "order by a.check_id,a.check_monitoring_zone, a.start_time"
get_suppressions = "select a.device, a.start_time, a.end_time from up_time_calculation.suppression_intervals a where (a.start_time < {0} and a.end_time > {1})"\
                " or (a.start_time >={2} and a.start_time  < {3})  order by a.device, a.start_time"
get_dev_monitor= "select distinct a.check_id, a.device from up_time_calculation.monitor_details a"
now_time = datetime.datetime.now()
now_date_ts=int(now_time.strftime("%s"))
now_date_ts_ms=now_date_ts*1000
while now_time > start_date:
    print now_time
    print start_date
    print now_time.month
    print start_date.month
    if now_time.month == start_date.month:
        print "Same month"
        supression_end_ts=now_date_ts
        end_ts=now_date_ts_ms
    print supression_end_ts
    print end_ts
    drop_partition_stmt="alter table  up_time_calculation.monitor_monthly_availability drop if exists partition (dt='{0}')".format(start_date.strftime('%Y-%m-%d'))
    print drop_partition_stmt
    #sqlContext.sql(drop_partition_stmt)
    os_cmd="hive -e "+'"'+drop_partition_stmt+'"'
    print os_cmd
    os.system(os_cmd)
    conf = SparkConf().setAppName("Monitor DownTime Calculation")
    conf.set("spark.driver.maxResultSize", "5g")
    sc = SparkContext(conf = conf)
    sqlContext= HiveContext(sc)
    current_suppression_stmn=get_suppressions.format(suppression_start_ts,suppression_start_ts,suppression_start_ts,supression_end_ts)
    suppressionMapBC=sc.broadcast(sqlContext.sql(current_suppression_stmn).map(lambda(a,b,c):(a,[(b,c)])).reduceByKey(lambda a,b:a+b).collectAsMap())
    current_dev_monitor_stmt=get_dev_monitor.format(start_ts, end_ts)
    print current_dev_monitor_stmt
    deviceMonitorMap=sqlContext.sql(current_dev_monitor_stmt).map(lambda (a,b): (a,b)).collectAsMap()
    deviceMonitorMapBC=sc.broadcast(deviceMonitorMap)

    ts_BC_tuple=(start_ts,end_ts,suppression_start_ts,supression_end_ts);
    print ts_BC_tuple
    ts_BC=sc.broadcast(ts_BC_tuple)


    #Calculate monthly up time
    #Get Monitors Creation date
    get_monitors_create_time="select id, created_at from maas.checks where created_at < {0}  UNION ALL select monitor_id, created_date from new_relic.monitors where created_date < {1}".format(end_ts,end_ts)
    print get_monitors_create_time
    monitorCreatedBC=sc.broadcast(sqlContext.sql(get_monitors_create_time).map(lambda (a,b): (a,b)).collectAsMap())
    number_of_seconds_in_month=0
    if now_time.month == start_date.month:
        number_of_seconds_in_month=now_date_ts_ms-start_ts
    else:
        number_of_seconds_in_month= (start_date+relativedelta(months=+1,days=-1)).day*86400*1000
    print "Number of seconds in the month"
    print number_of_seconds_in_month
    number_of_seconds_in_month_BC=sc.broadcast(number_of_seconds_in_month)



    current_get_monitor_errors_statement = get_monitor_data.format(start_ts, end_ts, start_ts, start_ts, end_ts)
    print current_get_monitor_errors_statement
    monitorerrorsRDD=sqlContext.sql(current_get_monitor_errors_statement)
    monitoravailabilityRDD=monitorerrorsRDD.map(lambda(x,y,s,e):((x,y),[(s,e)])).reduceByKey(lambda a,b:a+b).map(calculatemonitoruptime)
    print monitoravailabilityRDD.take(100)
    df2=sqlContext.createDataFrame(monitoravailabilityRDD, ['monitor', 'monitoring_zone','downtime_duration','downtime_duration_with_suppression', 'up_time','up_time_percent',
								 'up_time_with_suppression_percent' ])
    df2.registerTempTable('monitor_up_time')
    insert_monitor_up_time="insert overwrite table up_time_calculation.monitor_monthly_availability partition (dt='{0}') select a.monitor,a.monitoring_zone, b.check_target, "\
	"b.check_label, b.check_type, b.device_label,b.device_managed, b.device_uri,b.device, b.account ,a.downtime_duration,a.downtime_duration_with_suppression, a.up_time,"\
	"a.up_time_percent, a.up_time_with_suppression_percent, b.source_system_name from monitor_up_time as a, up_time_calculation.monitor_details as b "\
	" where a.monitor=b.check_id".format(start_date.strftime('%Y-%m-%d'))
    print insert_monitor_up_time
    sqlContext.sql(insert_monitor_up_time)
    start_date = end_date
    end_date = start_date + relativedelta(months=+1)
    start_ts = int(start_date.strftime("%s"))*1000
    end_ts = int(end_date.strftime("%s"))*1000
    suppression_start_ts= int(start_date.strftime("%s"))
    supression_end_ts=int(end_date.strftime("%s"))
    print start_date
    print end_date
    ts_BC_tuple=(start_ts,end_ts,suppression_start_ts,supression_end_ts)
    sc.stop()
with open (c.MONITOR_DATE_MARKER_FILE, 'w') as date_file:
    start_date =  start_date + relativedelta(months=-1)
    date_file.write(start_date.strftime('%Y-%m-%d'))


