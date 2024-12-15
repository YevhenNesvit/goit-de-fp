from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
}

dag = DAG(
    dag_id="batch_data_lake",
    default_args=default_args,
    schedule=None,
    catchup=False,
)

# Tasks
landing_to_bronze = SparkSubmitOperator(
    task_id="landing_to_bronze",
    application="/app/second_part/landing_to_bronze.py",
    dag=dag,
)

bronze_to_silver = SparkSubmitOperator(
    task_id="bronze_to_silver",
    application="/app/second_part/bronze_to_silver.py",
    dag=dag,
)

silver_to_gold = SparkSubmitOperator(
    task_id="silver_to_gold",
    application="/app/second_part/silver_to_gold.py",
    dag=dag,
)

# Task Dependencies
landing_to_bronze >> bronze_to_silver >> silver_to_gold
