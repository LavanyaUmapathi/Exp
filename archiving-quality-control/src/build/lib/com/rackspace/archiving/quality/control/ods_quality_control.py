'''
Created on Apr 14, 2016

@author: natasha.gajic
'''
import constants as cons
import quality_control_queries as q
import os
import sys
import requests
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = "C:\Natasha\Spark"
os.environ['PYTHONHOME']="C:\Natasha\Python2.7"
os.environ['PATH']=os.getenv("PATH")+"C:\Natasha\Python2.7"



def get_rdd_count(file_path):
    conf = SparkConf().setAppName("ODS Row Count")
    sc = SparkContext(conf = conf)
    return sc.textFile(file_path).count()
def orc_table_exists(db_name, table_name):
    resp = requests.get(cons.WEB_BASE_URL+'ddl/database/'+db_name+'/table/'+table_name+'?user.name='+cons.WEBHCAT_USERNAME)
    if resp.status_code == 200:
        return True
    else:
        return False
def main(argv):
    (db_name, table_name, by_partition, partition_date) = sys.argv
    output_file=open (table_name+'.txt',"w")
    is_orc_exist = orc_table_exists(db_name, table_name)
    if (by_partition):
        print "Query by Partition\n"
        if (partition_date.strip().length()==0):
            print "No partition date query all dates\n"
            (dir_name, sub_dir) = q.get_hdfs_files(db_name, table_name)
            for one_dir in sub_dir:
                query_date = one_dir.encode('ascii').split('=')[1]
                rdd_r_count = get_rdd_count('hdfs://'+dir_name+'/'+one_dir.encode('ascii'))
                orc_count=0
                if (is_orc_exist):
                    orc_r_count = q.get_orc_table_count_by_date(db_name, table_name, query_date)
                    sql_server_r_count=q.get_sql_server_record_count_by_date(db_name, table_name, query_date)
                    if (is_orc_exist):
                        output_file.write(query_date+','+str(sql_server_r_count)+','+str(rdd_r_count)+','+','+str(orc_r_count))
             
             
             
             
             
             