'''
Created on Mar 1, 2016

@author: natasha.gajic
Input arguments: ODS_SQL_SERVER_IP ODS_DB_NAME user_name password output_file

For selected ODS DB in MS SQL Server this script will print out information that helps to make
archiving decision. For each table the following is printed:
  table_owner, table_name, record_count, index_count, partitioned, incremental,comment,
  extraction_field, istimestamp, isdate,orcTable,onetimeload, index_information, first_load_date

This information is important to determine how to archive tables in Hadoop. There are 4 options:
    1. truncate & laod daily - appropriate for small static stables
    2. full load into partitioned table - keeping a full snapshot of the table by day
    3. incremental load into non-partitioned table - appropriate when there is a small number of
       records added daily (number of records < 100000/day)
    4. incremental load into partitioned table - appropriate when large number of records is
       added daily

For incremental load tables verify that the SQL Server table has a corresponding index.

At the end of the process the script will print all tables and record counts. Use this list to
create configuration file (csv) to be used in create_hive_ods.py.

For detail description of create_hive_ods.py please see the script.
'''

import sys

import src.hive.schema.maintenence.sqlserver_api as sql_api
import src.hive.schema.maintenence.constants as cons

conn = sql_api.get_connection(sys.argv[1], sys.argv[3], sys.argv[4], sys.argv[2])



table_list = sql_api.get_tables(conn)

print_table_list = []

for one_table in table_list:
    print"______________________________________"
    print one_table
    r_count = str(sql_api.get_record_count(conn, one_table))
    print one_table + ' Record count: ' + str(sql_api.get_record_count(conn, one_table))


    index_list = sql_api.get_indexes(conn, one_table)
    index_info = ''

    for one_index in index_list:
        print 'Index name: '+one_index[0]+' index columns: '+one_index[1]
        index_info = index_info+one_index[0] + '(' + one_index[1].replace(',', ' ') + ');'


    (t_owner, t_name) = one_table.split('.')
    sql_table_col = sql_api.get_columns(conn, t_name, t_owner)
    min_date = 'N/A'
    if (cons.DW_TIMESTAMP, 'datetime') in sql_table_col:
        min_date = sql_api.get_min_date(conn, t_name, cons.DW_TIMESTAMP, t_owner, False)


    print_table_list.append((one_table, str(r_count), str(len(index_list)), index_info, min_date))


print "*******************************************"
config_file = open(sys.argv[5], "w")
config_file.write('table_owner, table_name, record_count, index_count, partitioned, incremental, ' \
                  'comment, extraction_field, istimestamp, isdate,orcTable,onetimeload, ' \
                  'index_information, first_load_date\n')
for one_table in print_table_list:
    (t_owner, t_name) = one_table[0].split('.')
    create_orc_table = 'N'
    if long(one_table[1]) > 5000000:
        create_orc_table = 'Y'
    config_file.write(t_owner + ',' + t_name + ',' + one_table[1] + ',' + one_table[2] +
                      ',N,N,Backup for:' + t_name + ',' + cons.DW_TIMESTAMP + ',,,' +
                      create_orc_table + ',N,' + one_table[3] + ',' + one_table[4] + '\n')
