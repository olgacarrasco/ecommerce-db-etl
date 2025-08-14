from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add the DAGs folder to sys.path so it can find etl_utils
sys.path.append(os.path.dirname(__file__))

from etl_utils import run_etl  # This is your ETL function

default_args = {
    'owner': 'olga',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='csv_to_postgres_etl',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@daily',  # or None for manual
    catchup=False
) as dag:

    etl_task = PythonOperator(
        task_id='run_etl_task',
        python_callable=run_etl
    )

    etl_task
