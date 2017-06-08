from datetime import datetime, timedelta, time

from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator

# this variable should be a dictionary that maps user names to a list of queue names.
# use "default" for the default queue
# using the proper queue insures the ticket is refreshed on the correct node
users = Variable.get("ticket_refresh_users", deserialize_json=True)

yesterday_1500UTC = datetime.combine(datetime.today(), time(15, 0)) - timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': yesterday_1500UTC,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('ticket_refresher', default_args=default_args, schedule_interval=timedelta(days=1))


renew_cmd = 'sudo -u {user} kinit -k -t ~{user}/{user}.headless.keytab -l 72h -r 144h ' \
            '{user}@RACKSPACE.CORP'

for user, queues in users.iteritems():
    for queue in queues:
        BashOperator(task_id='refresh_ticket-{user}-{queue}'.format(user=user, queue=queue),
                     bash_command=renew_cmd.format(user=user),
                     queue=queue,
                     dag=dag)

