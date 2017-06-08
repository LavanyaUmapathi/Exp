'''
Created on Feb 17, 2016

@author: natasha.gajic
'''
import datetime
import sys

import pypyodbc as odbc
import constants as cons

def get_connection(dns, uname, passw, dbname):
    connstr = 'DRIVER={SQL Server};' + 'SERVER={server}; PORT=1433; Uid={uid}; Pwd={pwd};' \
              ' Database={db};'.format(
                  server=dns, uid=uname, pwd=passw, db=dbname)

    return odbc.connect(connstr)

def get_columns(conn, tname, towner):
    columnlst = []
    getcolumns = 'sp_columns @table_name={table}, @table_owner={owner}'.format(
        table=tname, owner=towner)
    with conn.cursor() as curr:
        curr.execute(getcolumns)

        for row in curr.fetchall():
            columnlst.append((row[3].lower().encode('ascii'), row[5].lower().encode('ascii')))

    return columnlst

def get_tables(conn):
    tab_lst = []
    get_tabs = 'sp_tables'
    with conn.cursor() as curr:
        curr.execute(get_tabs)

        for row in curr.fetchall():
            if row[3].encode('ascii') == cons.TABLE_KEYWORD:
                tab_lst.append(row[1].encode('ascii') + '.' + row[2].encode('ascii'))

    return tab_lst

def get_indexes(conn, table_name):
    index_lst = []
    with conn.cursor() as curr:
        cmd = 'sp_helpindex @objname=\'' + table_name + '\''

        curr.execute(cmd)
        try:
            for row in curr.fetchall():
                index_lst.append((row[0].encode('ascii'), row[2].encode('ascii')))
        except odbc.ProgrammingError:
            return index_lst

    return index_lst

def get_record_count(conn, table_name):
    with conn.cursor() as curr:
        (t_owner, t_name) = table_name.split('.')
        sql = 'select COUNT_BIG(*) from [' + t_owner + '].[' + t_name + ']'
        r_count = curr.execute(sql).fetchone()[0]

    return long(r_count)

def get_record_count_where(conn, table_name, where):
    with conn.cursor() as curr:
        (t_owner, t_name) = table_name.split('.')
        sql = 'select COUNT_BIG(*) from [' + t_owner + '].[' + t_name + ']' + where
        r_count = curr.execute(sql).fetchone()[0]

    return long(r_count)

def get_min_date(conn, table_name, date_field, table_owner, is_timestamp):
    with conn.cursor() as curr:
        try:
            sql = 'select min(' + date_field + ') from ' + table_owner + '.' + table_name
            min_date = curr.execute(sql).fetchone()[0]
            if is_timestamp:
                min_date = datetime.datetime.fromtimestamp(int(min_date)).strftime('%Y-%m-%d')
            return str(min_date)[:10]
        except:
            print "Error:", sys.exc_info()[0]
            return 'N/A'
