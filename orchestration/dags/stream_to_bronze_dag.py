from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 16),
    'retries': 0,
}

with DAG(
    dag_id='stream_to_bronze',
    default_args=default_args,
    schedule=None,  # Only run manually
    catchup=False,
) as dag:

    stream_to_bronze = BashOperator(
        task_id='run_stream_to_bronze',
        bash_command='docker exec spark-iceberg spark-submit /app/stream_to_bronze.py'
    ) 