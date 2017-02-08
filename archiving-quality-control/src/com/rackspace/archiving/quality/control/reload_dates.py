'''
Created on Apr 19, 2016

@author: natasha.gajic
'''
import os
for root,subFolder, files in os.walk("C:\Natasha\ods_quality_reports"):
    for one_file in files:
        print one_file
        if one_file.startswith("cloud") and one_file.endswith(".txt"):
            data=[line.strip() for line in open(os.path.join(root,one_file), 'r')]
            data=data[1:]
            history_date_list=[]
            ods_date_list=[]
            for  one_row in data:
                (date, ebi_ods_count, history_count,hadoop_count,orc_count) = tuple(one_row.split(","))
                if long(history_count)>0 and long(ebi_ods_count) == 0:
                    if long(history_count) - long(hadoop_count) > 0:
                        history_date_list.append(date)
                if long(ebi_ods_count)> 0:
                    if long(ebi_ods_count) - long(hadoop_count) > 0:
                        ods_date_list.append(date)
            if  history_date_list.__len__() > 0:
                file_name=one_file.split('.')[0]+"_history.sh"
                history_shell_script=open(os.path.join(root,file_name),"wb")
                for one_date in history_date_list:
                    history_shell_script.write("echo about to load table name "+one_file.split('.')[0]+" date: "+one_date+"\n")
                    history_shell_script.write("/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup_for_one_day_history.sh "+one_file.split('.')[0]+" dw_timestamp dbo false "+one_date+"\n")
                    history_shell_script.write("echo table name "+one_file.split('.')[0]+" date: "+one_date+" loaded\n")
                history_shell_script.close()
            if  ods_date_list.__len__() > 0:
                file_name=one_file.split('.')[0]+".sh"
                ods_shell_script=open(os.path.join(root,file_name),"wb")
                for one_date in ods_date_list:
                    ods_shell_script.write("echo about to load table name "+one_file.split('.')[0]+" date: "+one_date+"\n")
                    ods_shell_script.write("/home/airflow/airflow-jobs/scripts/cloud_events_usage/cloud_events_backup_for_one_day.sh "+one_file.split('.')[0]+" dw_timestamp dbo false "+one_date+"\n")
                    ods_shell_script.write("echo table name "+one_file.split('.')[0]+" date: "+one_date+" loaded\n")
                ods_shell_script.close()
restart_shell = open(os.path.join(root,"restartReload.sh"),"wb")
for root,subFolder, files in os.walk("C:\Natasha\ods_quality_reports"):
    for one_file in files:
        print one_file
        if one_file.startswith("cloud") and one_file.endswith(".sh"):
            restart_shell.write("/home/airflow/airflow-jobs/scripts/cloud_events_usage/reload/"+one_file+" > /tmp/reload/log/"+one_file.split('.')[0]+".log 2>&1 &\n")
            restart_shell.write("echo $! > /tmp/reload/pid/"+one_file.split('.')[0]+".pid\n")


                        
                