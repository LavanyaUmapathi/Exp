#!/bin/env python
"""
*** USE WITH CAUTION ***
Removes all traces of a DAG from the airflow database.
*** USE WITH CAUTION ***

Usage: rmdag.py <dag_id> <mysqlpassword>

NOTES:
- must be run from host with airflow database
- must restart airflow webserver after running this
- DAG code must be manually removed from DAGS_DIR
"""

import sys
import MySQLdb

dagid = sys.argv[1]
passwd = sys.argv[2]

templates = [
    'delete from xcom where dag_id = "{dagid}"',
    'delete from task_instance where dag_id = "{dagid}"',
    'delete from sla_miss where dag_id = "{dagid}"',
    'delete from log where dag_id = "{dagid}"',
    'delete from job where dag_id = "{dagid}"',
    'delete from dag_run where dag_id = "{dagid}"',
    'delete from dag where dag_id = "{dagid}"'
]

def execute(query):
    db = MySQLdb.connect(host="localhost", user="airflow", passwd=passwd, db="airflow")
    cur = db.cursor()
    cur.execute(query)
    print cur.rowcount
    db.commit()
    db.close()

for template in templates:
    query = template.format(dagid=dagid)
    print query
    execute(query)
