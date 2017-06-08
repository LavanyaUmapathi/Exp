'''
Created on Apr 13, 2016

@author: natasha.gajic
'''
from hdfs import InsecureClient
import pymssql as odbc
import constants as cons
import datetime
import sys

import pyhs2


def get_connection(db):

    with open(cons.SQL_CONNECTION_FILE,'r') as config_file:
        sql_server= config_file.readlines()
        return odbc.connect(sql_server[0].strip(),sql_server[1].strip(),sql_server[2].strip(),db)
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
            sql_txt='select count(*) from '+table_name+'_orc'
            print sql_txt
            cur.execute(sql_txt)
            r_count=cur.fetchone()
            print r_count
            return long(r_count[0])
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
                sql_txt='select count(*) from '+table_name+'_orc where `date`=\''+date_value+'\''
                print sql_txt
                cur.execute(sql_txt)
                r_count=cur.fetchone()
                print r_count
                return long(r_count[0])
            else:
                sql_txt='select count(*) from '+table_name+'_orc where dt=\''+date_value+'\''
                print sql_txt
                cur.execute(sql_txt)
                r_count=cur.fetchone()
                print r_count
                return long(r_count[0])
def get_orc_table_count_by_id(db_name, table_name, id_field, id_value):
    with pyhs2.connect(host=cons.HIVE_HOST,
                   port=cons.HIVE_PORT,
                   authMechanism="PLAIN",
                   user=cons.USER_NAME,
                   password='',
                   database=db_name) as conn:
        with conn.cursor() as cur:
            sql_txt='select count(*) from '+table_name+'_orc where '+id_field + '>'+id_value+''
            print sql_txt
            cur.execute(sql_txt)
            r_count=cur.fetchone()
            print r_count
            return long(r_count[0])

def get_hive_table_count(db_name, table_name):
    with pyhs2.connect(host=cons.HIVE_HOST,
                   port=cons.HIVE_PORT,
                   authMechanism="PLAIN",
                   user=cons.USER_NAME,
                   password='',
                   database=db_name) as conn:
        with conn.cursor() as cur:
            sql_txt='select count(*) from '+table_name
            print sql_txt
            cur.execute(cons.ADD_JAR)
            cur.execute(sql_txt)
            r_count=cur.fetchone()
            print r_count
            return long(r_count[0])
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
def get_hive_table_count_by_date(db_name, table_name, date_value):
    with pyhs2.connect(host=cons.HIVE_HOST,
                   port=cons.HIVE_PORT,
                   authMechanism="PLAIN",
                   user=cons.USER_NAME,
                   password='',
                   database=db_name) as conn:
        with conn.cursor() as cur:
            if db_name == 'cloud_usage_events':
                sql_txt='select count(*) from '+table_name+' where `date`=\''+date_value+'\''
                print sql_txt
                cur.execute(cons.ADD_JAR)
                cur.execute(sql_txt)
                r_count=cur.fetchone()
                print r_count
                return long(r_count[0])
            else:
                sql_txt='select count(*) from '+table_name+' where dt=\''+date_value+'\''
                print sql_txt
                cur.execute(cons.ADD_JAR)
                cur.execute(sql_txt)
                r_count=cur.fetchone()
                print r_count
                return long(r_count[0])
def get_hive_table_count_by_id(db_name, table_name, id_field, id_value):
    with pyhs2.connect(host=cons.HIVE_HOST,
                   port=cons.HIVE_PORT,
                   authMechanism="PLAIN",
                   user=cons.USER_NAME,
                   password='',
                   database=db_name) as conn:

        with conn.cursor() as cur:
            sql_txt='select count(*) from '+table_name+' where '+id_field + '>'+id_value+''
            print sql_txt
            cur.execute(cons.ADD_JAR)
            cur.execute(sql_txt)
            r_count=cur.fetchone()
            print r_count
            return long(r_count[0])


def get_sql_server_record_count(db_name, table_name):
    with get_connection(db_name) as conn:
        curr = conn.cursor()
        (t_owner,t_name) = table_name.split('.')
        sql_stmt='select COUNT_BIG(*) from ['+t_owner+'].['+t_name+'] WITH (NOLOCK)'
        print sql_stmt
        curr.execute(sql_stmt)
        r_count=long(curr.fetchone()[0])
        curr.close()
        print "SQL Server Result"
        print r_count
        return r_count
def get_sql_server_record_count_by_date(db_name, table_name, date_field_name, date_value):
    with get_connection(db_name) as conn:
        curr = conn.cursor()
        (t_owner,t_name) = table_name.split('.')
        start_date = datetime.datetime.strptime(date_value, "%Y-%m-%d")

        end_date = start_date + datetime.timedelta(days=1)
        sql_stmt='select COUNT_BIG(*) from ['+t_owner+'].['+t_name+'] WITH (NOLOCK) where '+date_field_name+' >=\''+start_date.isoformat()+'\' and '+date_field_name+' <\''+end_date.isoformat()+'\';'
        print sql_stmt
        curr.execute(sql_stmt)
        r_count=long(curr.fetchone()[0])
        curr.close()
        print "SQL Server Result"
        print r_count
        return r_count
def get_sql_server_record_count_by_timestamp(db_name, table_name, timestamp_name, date_value):
    with get_connection(db_name) as conn:
        curr = conn.cursor()
        (t_owner,t_name) = table_name.split('.')
        start_date = datetime.datetime.strptime(date_value, "%Y-%m-%d")

        end_date = start_date + datetime.timedelta(days=1)
        start_ts=str((start_date - datetime.datetime(1970, 1, 1)).total_seconds())
        end_ts=str((end_date - datetime.datetime(1970, 1, 1)).total_seconds())
        stmt='select COUNT_BIG(*) from ['+t_owner+'].['+t_name+'] WITH (NOLOCK) where '+timestamp_name +' >='+start_ts+' and '+timestamp_name+' <'+end_ts+';'
        print stmt
        curr.execute(stmt)
        r_count=long(curr.fetchone()[0])
        curr.close()
        print "SQL Server Result"
        print r_count
        return r_count

def get_sql_server_record_count_by_id(db_name, table_name, id_field, id_value):
    with get_connection(db_name) as conn:
        curr = conn.cursor()
        (t_owner,t_name) = table_name.split('.')
        sql_stmt='select COUNT_BIG(*) from ['+t_owner+'].['+t_name+'] WITH (NOLOCK) where '+id_field+' >\''+id_value+'\';'
        print sql_stmt
        curr.execute(sql_stmt)
        r_count=long(curr.fetchone()[0])
        curr.close()
        print "SQL Server Result"
        print r_count
        return r_count
