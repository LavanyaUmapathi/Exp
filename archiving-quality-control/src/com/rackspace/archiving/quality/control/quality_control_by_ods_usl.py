'''
Created on Apr 25, 2016

@author: natasha.gajic
'''
import sys
import os
import ods_quality_control as oqc
import constants as con
import datetime
import smtplib
from email.mime.text import MIMEText
def execute_re_run(ods_name, rerun_cmd):
    check_date=(datetime.date.today()-datetime.timedelta(1)).strftime("%Y-%m-%d")
    file_name=con.RERUN_DIR+"/"+ods_name+"_"+check_date+".sh"
    d_file_name=con.RERUN_DIR+"/"+ods_name+"_"+check_date+".sh_done"
    if (not os.path.exists(file_name) and (not os.path.exists(d_file_name))):
        rerun_cmd=rerun_cmd+con.AIRFLOW_JOBS_HOME+"/scripts/ods_archiving/checkDailyLoad.sh cloud_usage_events"
        print rerun_cmd
        f = open(file_name, 'w')
        f.write(rerun_cmd)
        f.close()
        os.chmod(file_name, 0755)
        print "Created ReRuning file"
        exit()


print "In USL DB DQ module"
shell_script_list=['/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_full_load_all.sh',
'/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_incremental2.sh',
'/home/airflow/airflow-jobs/scripts/usl_ods/usldb_ods_incremental1.sh']
msg_text=""
rerun_cmd=""
for one_script in shell_script_list:
    with open (one_script,'r') as input_script:
        for one_line in input_script:
            if not one_line.startswith("echo"):
                load_cmd = one_line.split()
                if  "full_" not in one_script:
                        (query_date,sql_server_r_count,rdd_r_count,orc_r_count)=oqc.check_table('usldb_ods',load_cmd[3]+'.'+load_cmd[1], 'True', load_cmd[2], (datetime.date.today()-datetime.timedelta(1)).strftime("%Y-%m-%d"))
                        if (sql_server_r_count > rdd_r_count):
                                rerun_cmd=rerun_cmd+one_line
                                msg_text=msg_text+"Record counts do not match for database {} table {} and date {} : SQL Server Count {}, Hadoop Count {}\n".format('usldb_ods',load_cmd[1],query_date,sql_server_r_count,rdd_r_count)
                else:
                        (query_date,sql_server_r_count,rdd_r_count,orc_r_count)=oqc.check_table('usldb_ods','dbo.'+load_cmd[1], 'False', '', '')
                        if (sql_server_r_count > rdd_r_count):
                                rerun_cmd=rerun_cmd+one_line
                                msg_text=msg_text+"Record counts do not match for database {} table {} and date {} : SQL Server Count {}, Hadoop Count {}\n".format('cloud_usage_events',load_cmd[1],query_date,sql_server_r_count,rdd_r_count)
s=smtplib.SMTP('localhost')
print msg_text
if msg_text == "":
    msg_text="Archiving successful"
    subject="{} archiving successful".format('usldb_ods')
else:
    execute_re_run("cloud_usage_events", rerun_cmd)
    subject="{} archiving error".format('usldb_ods')

msg =MIMEText(msg_text)
msg['Subject']=subject
msg['From']=con.EMAIL_FROM
msg['To']=con.EMAIL_TO
s.sendmail(con.EMAIL_FROM, con.EMAIL_TO, msg.as_string())
s.quit()

