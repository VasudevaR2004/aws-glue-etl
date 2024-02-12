# etl_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from etl_process import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL DAG for processing files from S3',
    schedule_interval=timedelta(days=1),
)

etl_task = PythonOperator(
    task_id='etl_process_task',
    python_callable=main,
    dag=dag,
)

etl_task
