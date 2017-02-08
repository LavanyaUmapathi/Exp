'''
Created on Apr 14, 2016

@author: natasha.gajic
'''
import constants as cons
import quality_control_queries as q
import os
import sys
import requests
from bokeh.io import output_file
import datetime
#from pyspark import SparkConf, SparkContext

'''os.environ['SPARK_HOME'] = "/usr/hdp/current/spark-client"
print os.environ['PATH']
os.environ['PATH']=os.getenv("PATH")+":/usr/hdp/current/spark-client/bin"
print os.environ['PATH']'''
from pyspark import SparkConf, SparkContext


'''def get_rdd_count(file_path):
    conf = SparkConf().setAppName("ODS Row Count")
    sc = SparkContext(conf = conf)
    return sc.textFile(file_path).count()'''
def orc_table_exists(db_name, table_name):
    resp = requests.get(cons.WEB_BASE_URL+'ddl/database/'+db_name+'/table/'+table_name+'?user.name='+cons.WEBHCAT_USERNAME)
    if resp.status_code == 200:
        return True
    else:
        return False
def query_full_table (db_name, t_name, table_name, is_orc_exist, output_file):
    hdfs_files = q.get_hdfs_files(db_name, t_name)
    for one_dir in hdfs_files:
        (dir_name, sub_dir, files) = one_dir
        rdd_r_count=0
        orc_r_count=0
        if (is_orc_exist):
              orc_r_count = q.get_orc_table_count(db_name, t_name)
              rdd_r_count=orc_r_count
        else:
            try:
                conf = SparkConf().setAppName("ODS Row Count")
                sc = SparkContext(conf = conf)

                rdd_r_count = sc.textFile('hdfs://'+dir_name.encode('ascii')).count()
            except:
              print sys.exc_info()
              rdd_r_count=0
            print "RDD result"
            print rdd_r_count
        
        
        sql_server_r_count=q.get_sql_server_record_count(db_name, table_name)
        if (is_orc_exist):
              output_file.write('NA'+','+str(sql_server_r_count)+','+str(rdd_r_count)+','+str(orc_r_count)+'\n')
        else:
              output_file.write('NA'+','+str(sql_server_r_count)+','+str(rdd_r_count)+','+'NA\n')
    

def check_table (db_name, table_name, by_partition, partition_date_field_name, partition_date):
    print db_name
    print table_name
    print by_partition
    print partition_date_field_name
    (s_name, t_name)=tuple(table_name.split('.'))
    with open (t_name+'.txt',"wb") as output_file:
            output_file.write('date,ebi_ods_count,hadoop_count,orc_count\n')
            is_orc_exist = orc_table_exists(db_name, t_name+'_orc')
            if (by_partition=='True'):
                print "Query by Partition\n"
                if (len(str(partition_date).strip())==0):
                    if (len(str(partition_date_field_name).strip()) > 0):
                        print "No partition date query all dates\n"
                        #(dir_name, sub_dir, files) = q.get_hdfs_files(db_name, t_name)
                        hdfs_files = q.get_hdfs_files(db_name, t_name)
                        for one_dir in hdfs_files:
                            (dir_name, sub_dir, files) = one_dir
                            for one_sub_dir in sub_dir:
                                query_date = one_sub_dir.encode('ascii').split('=')[1]
                                orc_r_count=0
                                rdd_r_count=0
                                if (is_orc_exist):
                                
                                    orc_r_count = q.get_orc_table_count_by_date(db_name, t_name, query_date)
                                    rdd_r_count=orc_r_count
                                else:        
                                    try:
                                        conf = SparkConf().setAppName("ODS Row Count")
                                        sc = SparkContext(conf = conf)
                                        rdd_r_count = sc.textFile('hdfs://'+dir_name.encode('ascii')+'/'+one_sub_dir.encode('ascii')).count()
                                    except:
                                        print sys.exc_info()
                                        rdd_r_count=0
                                        print "RDD result"
                                        print rdd_r_count
                            
                           
                                sql_server_r_count=q.get_sql_server_record_count_by_date(db_name, table_name, partition_date_field_name,query_date)
                                if (is_orc_exist):
                                    output_file.write(query_date+','+str(sql_server_r_count)+','+str(rdd_r_count)+','+str(orc_r_count)+'\n')
                                else:
                                    output_file.write(query_date+','+str(sql_server_r_count)+','+str(rdd_r_count)+','+'NA\n')
                    else:
                        print "Quality control for full table load into date partition\n"
                        partition_date = (datetime.date.today()-datetime.timedelta(1)).strftime("%Y-%m-%d")
                        print "Compare date:" + partition_date+"\n"
                        orc_r_count=0
                        rdd_r_count=0
                    
                        if (is_orc_exist):
                            orc_r_count = q.get_orc_table_count_by_date(db_name, t_name, partition_date)
                            rdd_r_count=orc_r_count
                        else:        
                            try:
                                hdfs_path=cons.HDFS_PATH+db_name+".db/"+t_name+"/dt="+partition_date
                                print hdfs_path
                                conf = SparkConf().setAppName("ODS Row Count")
                                sc = SparkContext(conf = conf)

                                rdd_r_count = sc.textFile(hdfs_path).count()
                            except:
                                print sys.exc_info()
                                rdd_r_count=0
                                print "RDD result"
                                print rdd_r_count
                    
                   
                        sql_server_r_count=q.get_sql_server_record_count(db_name, table_name)
                        if (is_orc_exist):
                            output_file.write(partition_date+','+str(sql_server_r_count)+','+str(rdd_r_count)+','+str(orc_r_count)+'\n')
                        else:
                            output_file.write(partition_date+','+str(sql_server_r_count)+','+str(rdd_r_count)+','+'NA\n')
                                
                                
                else:
                    print "Quality_control for date: "+partition_date+"\n"
                    
                    orc_r_count=0
                    rdd_r_count=0
                    
                    if (is_orc_exist):
                        orc_r_count = q.get_orc_table_count_by_date(db_name, t_name, partition_date)
                        rdd_r_count=orc_r_count
                    else:        
                        try:
                            hdfs_path=cons.HDFS_PATH+db_name+".db/"+t_name+"dt="+partition_date
                            print hdfs_path
                            rdd_r_count = sc.textFile(hdfs_path).count()
                        except:
                            print sys.exc_info()
                            rdd_r_count=0
                        print "RDD result"
                        print rdd_r_count
                    
                   
                    sql_server_r_count=q.get_sql_server_record_count_by_date(db_name, table_name, query_date)
                    if (is_orc_exist):
                            output_file.write(query_date+','+str(sql_server_r_count)+','+str(rdd_r_count)+','+str(orc_r_count)+'\n')
                    else:
                            output_file.write(query_date+','+str(sql_server_r_count)+','+str(rdd_r_count)+','+'NA\n')
                    
            else:
                print "Query full table\n"
                query_full_table (db_name, t_name, table_name, is_orc_exist, output_file)
    output_file.close()

