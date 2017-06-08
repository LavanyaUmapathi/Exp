import pyhs2
import time
import datetime
from dateutil.relativedelta import relativedelta

import mar_constants as cons

def create_line(row):
    line=row.check_id+cons.TAB+row.check_target+cons.TAB_row.check_label+cons.TAB+row.check_type+cons.TAB
    monitor_type=''
    if row.check_type=='remote.http':
        monitor_type='URL'
    else:
        monitor_type=str(row.check_type).split('.')[1].upper()
    line=line+monitor_type+cons.TAB+device_label+cons.TAB+device_managed+cons.TAB+device_uri+cons.TAB
    line=line+device+cons.TAB+account.split(':')[1]+cons.TAB+downtime_duration_cons.TAB
    line=line+downtime_duration_with_suppression_conns.TAB+up_time+cons.TAB+availability+cons.TAB+availability_with_suppression+cons.TAB+source_system_name
    return line+'\n'
run_date=sys.argv[0]
print e_date
now_time = datetime.datetime.now()
now_date_ts=int(now_time.strftime("%s"))
now_date_ts_ms=now_date_ts*1000

start_date=datetime.datetime.strptime(run_date.strip(), '%Y-%m-%d')
start_date=date(start_date.year, start_date.month,1)
print start_date
start_ts = int(datetime.datetime.strptime(run_date.strip(), '%Y-%m-%d').strftime("%s"))*1000
print start_ts
end_date = start_date+ relativedelta(months=+1)
end_ts =int(end_date.strftime("%s"))*1000
print end_date
print end_ts
number_of_seconds_in_month=0
if now_time.month == start_date.month:
        print "Same month"
        number_of_seconds_in_month=now_date_ts_ms-start_ts
        end_ts=now_date_ts_ms
        
else:
        number_of_seconds_in_month= (start_date+relativedelta(months=+1,days=-1)).day*86400*1000
print "Number of seconds in the month"
print number_of_seconds_in_month

select_errors=" select check_id, check_target, check_label, check_type, device_label,device_managed,\
             device_uri,device,account,downtime_duration,downtime_duration_with_suppression, \
             up_time,availability,availability_with_suppression, source_system_name \
             from up_time_calculation.service_monthly_availability where dt='{0}' and instr(account, 'hybrid')>0".format(start_date.strftime('%Y-%m-%d'))
print select_errors
select_good_full_month=" select a.check_id, a.check_target, a.check_label, a.check_type, a.device_label,a.device_managed,\
             a.device_uri,a.device,a.account,0,0, \
             {0},1,1, source_system_name \
             from up_time_calculation.monitor_details a, maas.checks b where \
             a.check_id=b.check_id and b.created_at < {1} and b.disabled=false and instr(account, 'hybrid')>0".format(number_of_seconds_in_month,start_ts)
             
print select_good_full_month
select_good_part_month=" select a.check_id, a.check_target, a.check_label, a.check_type, a.device_label,a.device_managed,\
             a.device_uri,a.device,a.account,0,0, \
             b.created-{0},1,1, source_system_name \
             from up_time_calculation.monitor_details a, maas.checks b where \
             a.check_id=b.check_id and b.created_at > {1} and created_at < {2} and b.disabled=false and instr(account, 'hybrid')>0".format(start_ts,start_ts, end_ts)
print select_good_part_month

with pyhs2.connect(host=cons.HIVE_HOST,
                   port=cons.HIVE_PORT,
                   authMechanism="PLAIN",
                   user=cons.USER_NAME,
                   password='',
                   database='up_time_calculation') as conn:
    with open ("max_report_{0}.txt".format(start_date.strftime('%Y-%m-%d')), 'w') as mar_file:
        mar_file.write(cons.TAB.join(cons.HEADER))
        with conn.cursor() as cur:
            cur.execute(select_errors)
            for row in cur:
               mar_file.write(create_line(row))
        with conn.cursor() as cur:
            cur.execute(select_good_full_month)
            for row in cur:
               mar_file.write(create_line(row))
        with conn.cursor() as cur:
            cur.execute(select_good_part_month)
            for row in cur:
               mar_file.write(create_line(row))
        
            
            
            
        

