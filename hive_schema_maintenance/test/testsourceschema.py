'''
Created on Feb 18, 2016

@author: natasha.gajic
'''
import os
import unittest

import hive.schema.maintenance.sqlserver_api as sch
import hive.schema.maintenance.webhcat_api as hcat


class TestHiveMaintenance(unittest.TestCase):

    def test_get_columns(self):
        
        oneresult = sch.get_columns(sch.get_connection('10.12.250.116', 'edwread', 'readedw', 'Cloud_Usage_Events'), 'vw_BACKUP_BANDWIDTHIN_USAGE_EVENTS'.lower(),'dbo')
        self.assertTrue(len(oneresult)>0)
        for (name, type) in oneresult:
            print (name.lower(), type)
    def test_get_databases(self):
        databases = hcat.get_databases()
        self.assertTrue(len(databases)>0)
        for db_name in databases:
            print db_name
    def test_get_tables(self):
        tables = hcat.get_tables('cloud_usage_events')
        self.assertTrue(len(tables)>0)
        for tab_name in tables:
            print tab_name
    def test_get_table_columns(self):
        columns = hcat.get_table_columns('cloud_usage_events','backup_bandwidthin_usage_events')
        self.assertTrue(len(columns)>0)
        for column in columns:
            print column
    def test_compare_table_columns(self):
        sqlcolumns = sch.get_columns(sch.get_connection('10.12.250.116', 'edwread', 'readedw', 'Cloud_Usage_Events'), 'vw_BACKUP_BANDWIDTHOUT_USAGE_EVENTS'.lower(), 'dbo')
        hcolumns= hcat.get_table_columns('cloud_usage_events','backup_bandwidthout_usage_events')
        
        self.assertTrue(len(sqlcolumns)== len(hcolumns))
        for i in range(0,len(sqlcolumns)):
            print 'SQL: {}, Hive:{}'.format(sqlcolumns[i][0], hcolumns[i][0])
            self.assertTrue(sqlcolumns[i][0]==hcolumns[i][0])
    def test_create_database(self):
        self.assertTrue(hcat.create_database('wcat_test','test WebHCat'))
    def test_create_table(self):
        column_list=[('field1', 'String'),('field2','String')]
        partition_list=[('p_field1', 'String')]
      #  {"external": true,"comment": "abc","columns": [{"name":"field1","type":"String"}, {"name":"field2","type":"String"}], "partitionedBy": [ {"name":"p_field1","type":"String"}],"format":{"rowFormat":{"serde":{"name": "com.bizo.hive.serde.csv.CSVSerde"}}}}
        self.assertTrue(hcat.create_table('wcat_test','test_table','abc' , column_list,partition_list, True))
    def test_drop_table(self):
        self.assertTrue(hcat.drop_table('wcat_test','test_table'))
    def test_get_sql_tables(self):
        self.assertTrue(len(sch.get_tables(sch.get_connection('10.12.250.116', 'edwread', 'readedw', 'Cloud_Usage_Events')))>0)
    def test_get_indexes(self):
        self.assertTrue(len(sch.get_indexes(sch.get_connection('10.12.250.116', 'edwread', 'readedw', 'Cloud_Usage_Events'),'dbo.CBS_USAGE_EVENTS'.lower()))>0)
        self.assertTrue(len(sch.get_indexes(sch.get_connection('10.12.250.116', 'edwread', 'readedw', 'BRM_ODS'),'dbo.staging_EVENT_BAL_IMPACTS_T_partition_20151009-134325'.lower()))>0)
    def test_get_record_count(self):
        self.assertTrue(sch.get_record_count(sch.get_connection('10.12.250.116', 'edwread', 'readedw', 'Cloud_Usage_Events'), 'dbo.CBS_USAGE_EVENTS'.lower())>0)
        self.assertTrue(sch.get_record_count(sch.get_connection('10.12.250.116', 'edwread', 'readedw', 'BRM_ODS'), 'dbo.staging_EVENT_BAL_IMPACTS_T_partition_20151009-134325'.lower())>0)
    def test_get_min_date(self):
        min_date=sch.get_min_date(sch.get_connection('10.12.250.116', 'edwread', 'readedw', 'Cloud_Usage_Events'), 'CBS_USAGE_EVENTS'.lower(), 'dw_timestamp', 'dbo', False)
        
        
        self.assertTrue(len(min_date)==10)  
    def test_tmp(self):
        
        tables = sch.get_tables(sch.get_connection('10.12.250.116', 'edwread', 'readedw', 'brm_ods'))
        self.assertTrue(len(tables)>0)
        for tab_name in tables:
            all_idx=sch.get_indexes(sch.get_connection('10.12.250.116', 'edwread', 'readedw', 'BRM_ODS'),tab_name)
            print str(len(all_idx))       
