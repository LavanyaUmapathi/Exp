#!/usr/bin/env python2.7

from datetime import datetime, timedelta, time

from airflow import DAG
from airflow.models import Variable
from airflow.operators import EmailOperator, PythonOperator
from pyhive import hive, exc


DAYS = 7
THRESHOLD = 2.0
hive_host = Variable.get("hive_host")

def records_for_days(curs, offset):
    sql = 'select count(*) from maas.metrics where dt = date_add(current_date, %d)' % offset
    try:
        curs.execute(sql)
        return curs.fetchone()[0]
    except exc.OperationalError:
        return 0

def maas_variance():
    conn = hive.Connection(host=hive_host)
    curs = conn.cursor()

    tot = 0
    for offset in range(0, DAYS):
        tot = tot + records_for_days(curs, -(3 + offset))
    cnt = records_for_days(curs, -2)

    avg = tot * 1.0 / DAYS
    variance = (cnt - avg) / avg

    return """
    === Metrics rows loaded ===
    Average   : {:>15,.0f} (last {} days)
    Yesterday : {:>15,} ({:.2%} variance)""".format(avg, DAYS, cnt, variance)

    # return 0 if abs(variance) < THRESHOLD / 100 else -1



yesterday_1000UTC = datetime.combine(datetime.today(), time(10, 0)) - timedelta(days=1)

default_args = {
    'owner': 'maas',
    'depends_on_past': False,
    'start_date': yesterday_1000UTC,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('variance-reporting', default_args=default_args, schedule_interval=timedelta(days=1))

t0 = PythonOperator(task_id='maas-metrics-variance',
                    python_callable=maas_variance,
                    dag=dag)

users = Variable.get("maas_variance_consumers", deserialize_json=True)

EmailOperator(task_id='maas-variance-email',
              to=users,
              subject='Variance Report',
              html_content='<pre>{{ task_instance.xcom_pull(task_ids="maas-metrics-variance") }}</pre>',
              dag=dag
             ).set_upstream(t0)

