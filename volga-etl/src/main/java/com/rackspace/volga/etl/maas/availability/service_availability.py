from pyspark import SparkConf, SparkContext, SQLContext

from pyspark.sql import HiveContext
from pyspark.sql.types import *
import constants as c
import time
import datetime
import os
from dateutil.relativedelta import relativedelta
from operator import itemgetter
#conf = SparkConf().setAppName("Service DownTime Calculation")
#conf.set("spark.driver.maxResultSize", "5g")
#sc = SparkContext(conf = conf)
#sqlContext= HiveContext(sc)

suppressionMapBC={}
deviceMonitorMapBC={}
ts_BC=()
monitorCreatedBC={}
number_of_sconds_in_month_BC=0


def findoverlap(et):

    #print et
    overlap_count=0
    overlap_start=0
    overlap_end=0

    monitor=et[0]


    errorlist=sorted(et[1],key=itemgetter(0))
    rcount=errorlist[0][2]-1
    first_start=0
    second_start=0
    first_end =0
    second_end=0
    rset=[()]
    #print rcount
    for i in range(0,len(errorlist)-1):
       #check if the interval started before the calculation month. If it is than maake it start at the begining of the month

       first_start=errorlist[i][0]
       second_start=errorlist[i+1][0]
       first_end=errorlist[i][1]
       second_end=errorlist[i+1][1]

       if first_start<ts_BC.value[0]:
           first_start=ts_BC.value[0]
       if second_start<ts_BC.value[0]:
           second_start=ts_BC.value[0]
       if first_end>ts_BC.value[1]:
            first_end=ts_BC.value[1]
       if second_end>ts_BC.value[1]:
            second_end=ts_BC.value[1]

       if rcount == 0:
           #Rcount is 1 each downtime counts
           rset.append((monitor,first_start, first_end,first_end - first_start))
           continue

       if overlap_start == 0:

           latest_start = max(first_start,second_start)
           earliest_end = min(first_end,second_end)

           if earliest_end - latest_start > 0:
               overlap_count = overlap_count+1
               overlap_start= latest_start
               overlap_end= earliest_end
           else:
               overlap_count=0
               overlap_start=0
               overlap_end=0
               #print 'no overlap'
       else:
           latest_start = max(overlap_start, second_start)
           earliest_end = min(overlap_end, second_end)
           if earliest_end - latest_start > 0:
               overlap_count = overlap_count+1
               overlap_start= latest_start
               overlap_end= earliest_end
           else:
               overlap_count=0
               overlap_start=0
               overlap_end=0
       if overlap_count==rcount:
            rset.append((monitor,overlap_start,overlap_end,overlap_end - overlap_start))
            overlap_count=0
            overlap_start=0
            overlap_end=0

    if len(rset) > 0:
        return rset
    else:
        return None
def findsupressionoverlap(et):
    overlap_count=0
    overlap_start=0
    overlap_end=0

    monitor=et[0]
    one_error=et
    rset=None
    device=None
    try:
        device=deviceMonitorMapBC.value[monitor]
    except:
        rset=(monitor,one_error[1],one_error[2], one_error[3], one_error[3])
        return rset

    dev_supressions=None
    try:
        dev_supressions = suppressionMapBC.value[device]
    except:
        rset=(monitor,one_error[1],one_error[2], one_error[3], one_error[3])
        return rset


    error_start=one_error[1]
    error_end =one_error[2]
    error_duration=one_error[3]

    s_found=False

    for one_supression in dev_supressions:
       s_start = one_supression[0]*1000
       s_end=one_supression[1]*1000
       if s_start <= error_start and s_end >= error_start:
            if s_end > error_end:
                rset=(monitor,one_error[1],one_error[2], one_error[3], 0)
                s_found=True
                break;
            else:
                error_start=s_end
                rset=(monitor,one_error[1],one_error[2], one_error[3],one_error[2]-error_start)
                s_found=True
                break
       else:
          if s_start > error_start and s_end < error_end:
                s_duration = s_end-s_start
                rset=(monitor,one_error[1],one_error[2], one_error[3],one_error[3]-s_duration)
                s_found=True
                break
          else:
                if s_start > error_start and s_start < error_end and s_end > error_end:
                     s_duration=s_start-error_start
                     rset=(monitor,one_error[1],one_error[2], one_error[3],s_duration)
                     s_found=True
                     break
    if not s_found:
         rset=(monitor,one_error[1],one_error[2], one_error[3], one_error[3])
    return rset
def calculateuptime(et):
    monitor=et[0]
    downtime_duration=0
    downtime_with_suppression_duration=0
    for one_downtime in et[1]:
        downtime_duration=downtime_duration+one_downtime[0]
        downtime_with_suppression_duration=downtime_with_suppression_duration+one_downtime[1];
    created_ts=0
    try:
        created_ts = monitorCreatedBC.value[monitor]
    except:
        return(monitor, downtime_duration,downtime_with_suppression_duration, 0, '{0:.6f}'.format(0),'{0:.6f}'.format(0))

    up_time=0;
    if created_ts < ts_BC.value[0]:
        up_time=number_of_sconds_in_month_BC.value
    else:
        up_time=ts_BC.value[1]-created_ts
    suppression_diff=downtime_duration-downtime_with_suppression_duration
    up_time_percent=float(1) - float(downtime_duration)/up_time
    aggreed_up_time=up_time-suppression_diff
    if aggreed_up_time==0:
        aggreed_up_time=up_time
    up_time_with_suppression_percent=float(1) - float(downtime_with_suppression_duration)/float(aggreed_up_time)
    return(monitor, downtime_duration,downtime_with_suppression_duration, up_time,'{0:.6f}'.format(up_time_percent), '{0:.6f}'.format(up_time_with_suppression_percent) )

run_date=''
with open (c.DATE_MARKER_FILE, 'r') as date_file:
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
load_monitors="insert overwrite table up_time_calculation.monitor_details  select * from up_time_calculation.vw_monitor_details"
print load_monitors
load_monitors_cmd="hive -e "+'"'+load_monitors+'" --hiveconf hive.execution.engine=mr --hiveconf hive.auto.convert.join=false'
print load_monitors_cmd
os.system(load_monitors_cmd)


get_data="select a.check_id, a.start_time,a.end_time,coalesce(b.rcount,1) as rcount from (select check_id , start_time, end_time from up_time_calculation.error_metrics_duration where (start
_time >= {0} and start_time <{1}) or (start_time < {2} and end_time > {3}) UNION ALL select check_id , start_time, 7227103875000 from up_time_calculation.open_errors where start_time < {4}
) a LEFT OUTER JOIN (select a.id, count(*) as rcount from (select id, substr(mz,3) as mzone from maas.checks lateral view explode(monitoring_zones_poll) mzTable as mz) as a group by a.id) b
 ON a.check_id=b.id   order by a.check_id, a.start_time"
get_monitor_data="select a.check_id,a.check_monitoring_zone, a.start_time,a.end_time from (select check_id ,check_monitoring_zone, start_time, end_time from up_time_calculation.error_metric
s_duration where (start_time >= {0} and start_time <{1}) or (start_time < {2} and end_time > {3}) UNION ALL select check_id ,check_monitoring_zone, start_time, 7227103875000 from up_time_ca
lculation.open_errors where start_time < {4} ) a order by a.check_id,a.check_monitoring_zone, a.start_time"
get_suppressions = "select a.device, a.start_time, a.end_time from up_time_calculation.suppression_intervals a where (a.start_time < {0} and a.end_time > {1}) or (a.start_time >={2} and a.s
tart_time  < {3})  order by a.device, a.start_time"
get_dev_monitor= "select distinct a.check_id, a.device from up_time_calculation.monitor_details a"
now_time = datetime.datetime.now()
now_date_ts=int(now_time.strftime("%s"))
now_date_ts_ms=now_date_ts*1000
while now_time > start_date:
    print get_data
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
    drop_partition_stmt="alter table  up_time_calculation.service_downtime_with_suppression drop if exists partition (dt='{0}');alter table  up_time_calculation.service_monthly_availability
 drop if exists partition (dt='{1}');".format(start_date.strftime('%Y-%m-%d'),start_date.strftime('%Y-%m-%d'))
    print drop_partition_stmt
    #sqlContext.sql(drop_partition_stmt)
    os_cmd="hive -e "+'"'+drop_partition_stmt+'"'
    print os_cmd
    os.system(os_cmd)
    conf = SparkConf().setAppName("Service DownTime Calculation")
    conf.set("spark.driver.maxResultSize", "5g")
    sc = SparkContext(conf = conf)
    sqlContext= HiveContext(sc)

    current_suppression_stmn=get_suppressions.format(suppression_start_ts,suppression_start_ts,suppression_start_ts,supression_end_ts)
    print current_suppression_stmn
    suppressionMapBC=sc.broadcast(sqlContext.sql(current_suppression_stmn).map(lambda(a,b,c):(a,[(b,c)])).reduceByKey(lambda a,b:a+b).collectAsMap())
    current_dev_monitor_stmt=get_dev_monitor.format(start_ts, end_ts)
    print current_dev_monitor_stmt
    deviceMonitorMap=sqlContext.sql(current_dev_monitor_stmt).map(lambda (a,b): (a,b)).collectAsMap()
    deviceMonitorMapBC=sc.broadcast(deviceMonitorMap)

    ts_BC_tuple=(start_ts,end_ts,suppression_start_ts,supression_end_ts);
    print ts_BC_tuple
    ts_BC=sc.broadcast(ts_BC_tuple)

    current_statement = get_data.format(start_ts, end_ts, start_ts, start_ts, end_ts)
    print current_statement
    errorsRDD=sqlContext.sql(current_statement)

    errorsTupleRDD=errorsRDD.rdd.map(lambda (x,y,z,a): (x,[(y,z,a)])).reduceByKey(lambda a,b: a+b).flatMap(findoverlap).filter(lambda x: len(x) > 0)
    print errorsTupleRDD.take(100)
    errorsWithSuppressionRDD=errorsTupleRDD.map(findsupressionoverlap).filter(lambda x: len(x) > 0)
    print errorsWithSuppressionRDD.take(100)

    df = sqlContext.createDataFrame(errorsWithSuppressionRDD, ['monitor','start_time','end_time','duration','duration_with_suppression'])

    df.registerTempTable('dt_durations')
    insert_stmt = "insert overwrite table up_time_calculation.service_downtime_with_suppression partition (dt='{0}') select a.monitor, b.check_target, b.check_label, b.check_type, b.device_
label,b.device_managed, b.device_uri,b.device, b.account ,a.start_time, a.end_time, a.duration , a.duration_with_suppression, b.source_system_name from  dt_durations as a, up_time_calculati
on.monitor_details as b  where a.monitor=b.check_id".format(start_date.strftime('%Y-%m-%d'))

    print insert_stmt
    sqlContext.sql(insert_stmt)
    #Remove suppression and monitor_device BCs
    suppressionMapBC.unpersist(blocking= True)
    deviceMonitorMapBC.unpersist(blocking=True)
    #Calculate monthly up time
    #Get Monitors Creation date
    get_monitors_create_time="select id, created_at from maas.checks where created_at < {0}".format(end_ts)
    print get_monitors_create_time;
    monitorCreatedBC=sc.broadcast(sqlContext.sql(get_monitors_create_time).map(lambda (a,b): (a,b)).collectAsMap())
    number_of_sceonds_in_month=0
    if now_time.month == start_date.month:
        number_of_sceonds_in_month=now_time.day*86400*1000
    else:
        number_of_sceonds_in_month= (start_date+relativedelta(months=+1,days=-1)).day*86400*1000
    print number_of_sceonds_in_month
    number_of_sconds_in_month_BC=sc.broadcast(number_of_sceonds_in_month)
    uptimeRDD=errorsWithSuppressionRDD.map(lambda(x,y,z,d,ds):(x,[(d,ds)])).reduceByKey(lambda a,b: a+b).map(calculateuptime)
    print uptimeRDD.take(100)
    df1 = sqlContext.createDataFrame(uptimeRDD, ['monitor', 'downtime_duration','downtime_duration_with_suppression', 'up_time','up_time_percent', 'up_time_with_suppression_percent' ])

    df1.registerTempTable('dt_up_time')
    insert_up_time_stmt = "insert overwrite table up_time_calculation.service_monthly_availability partition (dt='{0}') select a.monitor, b.check_target, b.check_label, b.check_type, b.devi
ce_label,b.device_managed, b.device_uri,b.device, b.account ,a.downtime_duration,a.downtime_duration_with_suppression, a.up_time,a.up_time_percent, a.up_time_with_suppression_percent, b.sou
rce_system_name from dt_up_time as a, up_time_calculation.monitor_details as b  where a.monitor=b.check_id".format(start_date.strftime('%Y-%m-%d'))

    print insert_up_time_stmt
    sqlContext.sql(insert_up_time_stmt)

    start_date = end_date
    end_date = start_date + relativedelta(months=+1)
    start_ts = int(start_date.strftime("%s"))*1000
    end_ts = int(end_date.strftime("%s"))*1000
    suppression_start_ts= int(start_date.strftime("%s"))
    supression_end_ts=int(end_date.strftime("%s"))
    print start_date
    print end_date
    ts_BC_tuple=(start_ts,end_ts,suppression_start_ts,supression_end_ts);
    sc.stop()
with open (c.DATE_MARKER_FILE, 'w') as date_file:
    start_date =  start_date + relativedelta(months=-1)
    date_file.write(start_date.strftime('%Y-%m-%d'))