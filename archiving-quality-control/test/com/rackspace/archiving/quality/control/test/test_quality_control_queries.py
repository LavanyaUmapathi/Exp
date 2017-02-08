'''
Created on Apr 13, 2016

@author: natasha.gajic
'''
import os
import unittest
import com.rackspace.archiving.quality.control.quality_control_queries as q

class TestHiveMaintenence(unittest.TestCase):

    def test_get_sql_server_record_count(self):
        self.assertTrue(q.get_sql_server_record_count('cbs_ods','dbo.quota_classes_cinderdb')>0)
    def test_get_sql_server_record_count_by_date(self):
        self.assertTrue(q.get_sql_server_record_count_by_date('brm_ods','dbo.event_dlay_rax_lsrvr_bwout_t','2016-04-02')>0)
    def test_get_sql_server_record_count_by_timestamp(self):
        self.assertTrue(q.get_sql_server_record_count_by_timestamp('brm_ods','dbo.journal_t','mod_t', '2016-04-02')>0)
    def test_get_sql_server_record_count_by_id(self):
        self.assertTrue(q.get_sql_server_record_count_by_id('cbs_ods','dbo.account_lunrdb','id', '998900')>0)
    def test_get_orc_table_count(self):
        self.assertTrue(q.get_orc_table_count('brm_ods','dd_objects_t_orc')>0)
      