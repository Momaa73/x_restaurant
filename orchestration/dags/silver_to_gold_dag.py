from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 16),
    'retries': 1,
}

with DAG(
    dag_id='silver_to_gold',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    silver_to_gold = BashOperator(
        task_id='run_silver_to_gold',
        bash_command='docker exec spark-iceberg spark-submit /app/silver_to_gold.py'
    ) 