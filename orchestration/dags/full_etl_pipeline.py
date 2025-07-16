from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 16),
    'retries': 1,
}

with DAG(
    dag_id='full_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    bronze_to_silver = BashOperator(
        task_id='run_bronze_to_silver',
        bash_command='docker exec spark-iceberg spark-submit /app/bronze_to_silver.py'
    )

    silver_to_gold = BashOperator(
        task_id='run_silver_to_gold',
        bash_command='docker exec spark-iceberg spark-submit /app/silver_to_gold.py'
    )

    bronze_to_silver >> silver_to_gold 