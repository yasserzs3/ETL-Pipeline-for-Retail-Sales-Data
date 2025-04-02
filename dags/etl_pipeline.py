"""
ETL Pipeline DAG for Sales Data Processing.
This DAG orchestrates the entire ETL process from extraction to loading.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.extraction import extract_data
from scripts.transformation import transform_data
from scripts.loading import load_data

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 1),
}

# Instantiate the DAG
dag = DAG(
    'retail_sales_etl',
    default_args=default_args,
    description='ETL pipeline for processing sales data',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'sales'],
)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task 