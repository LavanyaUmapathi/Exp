'''
Created on Apr 13, 2016

@author: natasha.gajic
'''
from hdfs import InsecureClient
import pypyodbc as odbc
import constants as cons
import datetime
import sys
#sys.path.append('C:\Natasha\Anaconda2\hive-thrift-py-0.0.1\dist\hive_thrift_py-0.0.1-py2.7.egg')
import pyhs2

'''from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol'''

#from setuptools.command.setopt import config_file
def get_connection(db):
   # connstr='DSN={dns}; Uid={uid}; Pwd={pwd}; Database={db}'.format(dns=dns,uid=uname,pwd=passw,db=db)
    with open(cons.SQL_CONNECTION_FILE,'r') as config_file:
        sql_server= config_file.readlines()
        connstr='DRIVER={SQL Server};'+ 'SERVER={server}; Uid={uid}; Pwd={pwd}; Database={db}'.format(server=sql_server[0].strip(),uid=sql_server[1].strip(),pwd=sql_server[2].strip(),db=db)
        print connstr
        return odbc.connect(connstr)
def get_hdfs_files(db_name, table_name):
     client = InsecureClient(cons.BASE_URL, user=cons.USER_NAME)
     content = client.walk(cons.HDFS_PATH+db_name+'.db/'+table_name,1)
     return content
def get_orc_table_count(db_name, table_name):
    with pyhs2.connect(host=cons.HIVE_HOST,
                   port=cons.HIVE_PORT,
                   authMechanism="PLAIN",
                   user=cons.USER_NAME,
                   password='',
                   database=db_name) as conn:
        with conn.cursor() as cur:
            sql_txt='select count(*) from '+table_name+'_orc ;'       
            return cur.execute(sql_txt).fetchone()
    '''transport = TSocket.TSocket(cons.HIVE_HOST, cons.HIVE_PORT)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
 
    client = ThriftHive.Client(protocol)
    transport.open()
    sql_txt='select count(*) from '+table_name+'_orc ;'     
    client.execute(sql_txt)
    r_count = client.fetchOne()
   
    transport.close()
    return r_count'''
def get_orc_table_count_by_date(db_name, table_name, date_value):
    with pyhs2.connect(host=cons.HIVE_HOST,
                   port=cons.HIVE_PORT,
                   authMechanism="PLAIN",
                   user=cons.USER_NAME,
                   password='',
                   database=db_name) as conn:
        with conn.cursor() as cur:
            if db_name == 'cloud_usage_events':
                sql_txt='select count(*) from '+table_name+'_orc where `date`=\''+date_value+'\';'       
                return long(cur.execute(sql_txt).fetchone())
            else:
                sql_txt='select count(*) from '+table_name+'_orc where dt=\''+date_value+'\';'       
                return long(cur.execute(sql_txt).fetchone())
def get_orc_table_count_by_id(db_name, table_name, id_field, id_value):
    with pyhs2.connect(host=cons.HIVE_HOST,
                   port=cons.HIVE_PORT,
                   authMechanism="PLAIN",
                   user=cons.USER_NAME,
                   password='',
                   database=db_name) as conn:
        with conn.cursor() as cur:         
            sql_txt='select count(*) from '+table_name+'_orc where '+id_field + '>'+id_value+';'       
            return long(cur.execute(sql_txt).fetchone())

def get_sql_server_record_count(db_name, table_name):
    with get_connection(db_name) as conn:
        curr = conn.cursor()
        (t_owner,t_name) = table_name.split('.')
        r_count=curr.execute('select COUNT_BIG(*) from ['+t_owner+'].['+t_name+']').fetchone()[0]
        curr.close()
        return long(r_count)
def get_sql_server_record_count_by_date(db_name, table_name, date_value):
    with get_connection(db_name) as conn:
        curr = conn.cursor()
        (t_owner,t_name) = table_name.split('.')
        start_date = datetime.datetime.strptime(date_value, "%Y-%m-%d")

        end_date = start_date + datetime.timedelta(days=1)
        r_count=long(curr.execute('select COUNT_BIG(*) from ['+t_owner+'].['+t_name+'] where dw_timestamp >=\''+start_date.isoformat()+'\' and dw_timestamp <\''+end_date.isoformat()+'\';').fetchone()[0])
        curr.close()
        return r_count
def get_sql_server_record_count_by_timestamp(db_name, table_name, timestamp_name, date_value):
    with get_connection(db_name) as conn:
        curr = conn.cursor()
        (t_owner,t_name) = table_name.split('.')
        start_date = datetime.datetime.strptime(date_value, "%Y-%m-%d")

        end_date = start_date + datetime.timedelta(days=1)
        start_ts=str((start_date - datetime.datetime(1970, 1, 1)).total_seconds())
        end_ts=str((end_date - datetime.datetime(1970, 1, 1)).total_seconds())
        stmt='select COUNT_BIG(*) from ['+t_owner+'].['+t_name+'] where '+timestamp_name +' >='+start_ts+' and '+timestamp_name+' <'+end_ts+';'
        print stmt
        return long(curr.execute(stmt).fetchone()[0])
 
def get_sql_server_record_count_by_id(db_name, table_name, id_field, id_value):
    with get_connection(db_name) as conn:
        curr = conn.cursor()
        (t_owner,t_name) = table_name.split('.')
        r_count= long(curr.execute('select COUNT_BIG(*) from ['+t_owner+'].['+t_name+'] where '+id_field+' >\''+id_value+'\';').fetchone()[0])     
        return r_count
       
    
        
            
                
     
    