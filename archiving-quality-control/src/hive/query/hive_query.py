'''
Created on Mar 15, 2016

@author: natasha.gajic
'''
import pyhs2

output_file=open("rawxml_count.txt",'w')


with pyhs2.connect(host='localhost',
                   port=10000,
                   authMechanism="PLAIN",
                   user='natasha',
                   password='',
                   database='cloud_usage_events') as conn:
    with conn.cursor() as cur:
        with open("rawxml.csv",'r') as config_file:
            config_data=[line.split(',') for line in config_file]
            for config_line in config_data:
                (table_name, r_count, start_date, end_date) = config_line
                t_name=table_name.split('.')[1].lower()
                (s_mon,s_day,s_year)=start_date.split('/')
                (e_mon,e_day,e_year) = end_date.split('/')
                if len(s_mon) == 1:
                    s_mon='0'+s_mon
                if len(e_mon) == 1:
                    e_mon='0'+e_mon
                if len(s_day) == 1:
                    s_day='0'+s_day
                if len(e_day) == 1:
                    e_day='0'+e_day
                sql_txt='select\'' +t_name+' sql server rCount: '+str(r_count)+' hadoop rCount: \',count(*) from cloud_usage_events.'+t_name+' where `date`>=\''+s_year+'-'+s_mon+'-'+s_day+'\' and `date` <= \''+e_year.strip()+'-'+e_mon+'-'+e_day+'\';'
        
                print sql_txt
                output_file.write(sql_txt)
                output_file.write('\n')
                
                cur.execute(sql_txt)
        
                for i in cur.fetch():
                    output_file.write(i)
                    output_file.write('\n')
output_file.close()
                