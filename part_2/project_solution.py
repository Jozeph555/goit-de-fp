from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

dag = DAG(
    'Yanvari_batch_data_lake_etl',
    default_args=default_args,
    description='End-to-End Batch Data Lake ETL Pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['yanvari'],
)

landing_to_bronze_task = SparkSubmitOperator(
    application=os.path.join(BASE_DIR, 'landing_to_bronze.py'),
    task_id='landing_to_bronze',
    conn_id='spark-default',
    verbose=True,
    dag=dag,
)

bronze_to_silver_task = SparkSubmitOperator(
    application=os.path.join(BASE_DIR, 'bronze_to_silver.py'),
    task_id='bronze_to_silver',
    conn_id='spark-default',
    verbose=True,
    dag=dag,
)

silver_to_gold_task = SparkSubmitOperator(
    application=os.path.join(BASE_DIR, 'silver_to_gold.py'),
    task_id='silver_to_gold',
    conn_id='spark-default',
    verbose=True,
    dag=dag,
)

landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task
