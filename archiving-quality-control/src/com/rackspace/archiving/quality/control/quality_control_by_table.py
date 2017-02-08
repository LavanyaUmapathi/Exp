'''
Created on Apr 25, 2016

@author: natasha.gajic
'''
import sys
from com.rackspace.archiving.quality.control import ods_quality_control_report as oqc

print "AAAAAAA\n"
partition_date=''
db_name=''
table_name=''
by_partition=''
partition_date=''
partition_date_field_name=''
if len(sys.argv) == 6:
     (db_name, table_name, by_partition, partition_date_field_name,partition_date) = tuple(sys.argv[1:])
     
else:
    if len(sys.argv) == 5:
        (db_name, table_name ,by_partition,partition_date_field_name) = tuple(sys.argv[1:])
    else:
        (db_name, table_name ,by_partition)=tuple(sys.argv[1:])
oqc.check_table (db_name, table_name, by_partition, partition_date_field_name, partition_date)
