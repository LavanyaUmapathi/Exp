import airflow
import sys

k=sys.argv[1]
v=sys.argv[2]

airflow.models.Variable.set(k,v)
