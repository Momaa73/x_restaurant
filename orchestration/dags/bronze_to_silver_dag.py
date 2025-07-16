from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 16),
    'retries': 1,
}

with DAG(
    dag_id='bronze_to_silver',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    bronze_to_silver = BashOperator(
        task_id='run_bronze_to_silver',
        bash_command='docker exec spark-iceberg spark-submit /app/bronze_to_silver.py'
    ) 