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
        rerun_cmd=rerun_cmd+con.AIRFLOW_JOBS_HOME+"/scripts/ods_archiving/checkDailyLoad.sh "+ ods_name
        print rerun_cmd
        f = open(file_name, 'w')
        f.write(rerun_cmd)
        f.close()
        os.chmod(file_name, 0755)
        print "Created ReRuning file"
        exit()

print "In DQ module\n"
ods_name = sys.argv[1]
print ods_name+"\n"
ODS_DIR=con.AIRFLOW_JOBS_HOME+"/scripts/"+ods_name
print ODS_DIR
msg_text=""
subject=""
rerun_cmd=""
for file in os.listdir(ODS_DIR):
    print file
    if (file.endswith(".sh") and file.startswith("ods_load_batch")):
        with open(ODS_DIR+"/"+file,"rb") as script_file:
            for one_table in script_file:
                print one_table
                if ( not (one_table.startswith(con.COMMENT_SIMBOL))):
                    load_cmd = one_table.split()
                    (query_date,sql_server_r_count,rdd_r_count,orc_r_count)=('','','','')
                    if load_cmd[0].endswith(con.FULL_TABLE_LOAD_SCRIPT):
                        (query_date,sql_server_r_count,rdd_r_count,orc_r_count)=oqc.check_table(load_cmd[1],load_cmd[3]+'.'+load_cmd[2], 'False','','')
                    elif load_cmd[0].endswith(con.INCREMENTAL_NON_PARTITION_TABLE_LOAD_SCRIPT):
                        (query_date,sql_server_r_count,rdd_r_count,orc_r_count)=oqc.check_table(load_cmd[1],load_cmd[4]+'.'+load_cmd[2], 'False','','')
                    elif load_cmd[0].endswith(con.FULL_PARTITIONED_TABLE_LOAD_SCRIPT):
                        (query_date,sql_server_r_count,rdd_r_count,orc_r_count)=oqc.check_table(load_cmd[1],load_cmd[4]+'.'+load_cmd[2], 'True','','')
                    elif load_cmd[0].endswith(con.INCREMETAL_PARTITIONED_TABLE_LOAD_SCRIPT):
                        (query_date,sql_server_r_count,rdd_r_count,orc_r_count)=oqc.check_table(load_cmd[1],load_cmd[4]+'.'+load_cmd[2], 'True', load_cmd[3], (datetime.date.today()-datetime.timedelta(1)).strftime("%Y-%m-%d"))

                    if (sql_server_r_count > rdd_r_count):
                        rerun_cmd=rerun_cmd+one_table
                        if (query_date != 'NA'):
                            msg_text=msg_text+"Record counts do not match for database {} table {} and date {} : SQL Server Count {}, Hadoop Count {}\n".format(load_cmd[1],load_cmd[2],query_date,sql_server_r_count,rdd_r_count)
                        else:
                            msg_text=msg_text+"Record counts do not match for database {} table {}  : SQL Server Count {}, Hadoop Count {}\n".format(load_cmd[1],load_cmd[2],sql_server_r_count,rdd_r_count)


s=smtplib.SMTP('localhost')
print msg_text
if msg_text == "":
    msg_text="Archiving successful"
    subject="{} archiving successful".format(ods_name)
else:
    execute_re_run(ods_name, rerun_cmd)
    subject="{} archiving error".format(ods_name)

msg =MIMEText(msg_text)
msg['Subject']=subject
msg['From']=con.EMAIL_FROM
msg['To']=con.EMAIL_TO
s.sendmail(con.EMAIL_FROM, con.EMAIL_TO, msg.as_string())
s.quit()