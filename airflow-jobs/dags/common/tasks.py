from airflow.operators import BashOperator
from airflow.utils import TriggerRule


def alert_task(log, prefix, dag):
    return BashOperator(
        task_id='alert_task',
        bash_command='alert-to-pager-duty \
-l {log} -s {prefix} < {log}'.format(
            log=log, prefix=prefix),
        trigger_rule=TriggerRule.ONE_FAILED,
        dag=dag)
