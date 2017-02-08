'''
Created on Apr 6, 2016

@author: natasha.gajic
'''

import datetime
import pypyodbc as odbc
import hive.schema.maintenence.sqlserver_api as sch

run_date=datetime.datetime(2013,07,20)
end_date=datetime.datetime (2015,10,10)
run_date += datetime.timedelta(days=1)

conn= sch.get_connection('10.12.250.116', 'edwread', 'readedw', 'brm_ods')

output_file = open('r_count_output.txt','w')
while (run_date < end_date):
    next_date = run_date+datetime.timedelta(days=1)
    stmt= "select '"+run_date.__str__()[0:10]+ "', count_big(*) from [dbo].[EVENT_BAL_IMPACTS_T_Archive] where dw_timestamp >= '"+run_date.__str__()+"' and dw_timestamp < '"+next_date.__str__()+"';"
    print stmt
    curr=conn.cursor()
    curr.execute(stmt)
    for row in curr.fetchall(): 
        o=row[0].encode('ascii')+","+ str(row[1]).encode('ascii')+"\n";
        print o
        output_file.write(o)
        run_date=next_date
    
output_file.close()
    