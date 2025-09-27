from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator

# DAG configuration
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'batch_data_lake_etl',
    default_args=default_args,
    description='End-to-End Batch Data Lake ETL Pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['data-lake', 'etl', 'spark', 'athletes'],
)

# Start task (dummy)
start_task = DummyOperator(
    task_id='start_etl_pipeline',
    dag=dag,
)

# Task 1: Landing to Bronze
landing_to_bronze_task = SparkSubmitOperator(
    task_id='landing_to_bronze',
    application='dags/yanvari/landing_to_bronze.py',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
    application_args=[],
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    }
)

# Task 2: Bronze to Silver
bronze_to_silver_task = SparkSubmitOperator(
    task_id='bronze_to_silver',
    application='dags/yanvari/bronze_to_silver.py',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
    application_args=[],
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    }
)

# Task 3: Silver to Gold
silver_to_gold_task = SparkSubmitOperator(
    task_id='silver_to_gold',
    application='dags/yanvari/silver_to_gold.py',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
    application_args=[],
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    }
)

# End task (dummy)
end_task = DummyOperator(
    task_id='end_etl_pipeline',
    dag=dag,
)

# Define task sequence
start_task >> landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task >> end_task

# Additional configuration for better understanding of dependencies
landing_to_bronze_task.doc_md = """
## Landing to Bronze Task
This task downloads raw data from FTP server and converts it to Parquet format.

**Functionality:**
- Download athlete_bio.csv and athlete_event_results.csv
- Convert CSV → Parquet
- Save to bronze/ directory
"""

bronze_to_silver_task.doc_md = """
## Bronze to Silver Task
This task cleans and processes data from bronze layer.

**Functionality:**
- Clean text fields
- Remove duplicates
- Save to silver/ directory
"""

silver_to_gold_task.doc_md = """
## Silver to Gold Task
This task creates analytical data from silver layer.

**Functionality:**
- Join athlete_bio and athlete_event_results
- Group by sport, medal, sex, country_noc
- Calculate average weight and height
- Add timestamp
- Save to gold/ directory
"""