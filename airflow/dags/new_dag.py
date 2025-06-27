from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from airflow.models import Variable

default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False
}


# functions
def def1():
    print('hello')


# DAG
with DAG(
    dag_id = 'new_dag',
    default_args = default_args,
    schedule = None,
    catchup = False,
    description = 'This DAG is a testing one.',
    tags = ['database', 'postgres']
) as dag:
    
    # tasks
    task1 = PythonOperator(
        task_id = 'task1',
        python_callable = def1,
    )

    

    # dependencies
    task1