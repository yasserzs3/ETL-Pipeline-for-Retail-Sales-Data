"""
ETL Pipeline DAG for Sales Data Processing.
This DAG orchestrates the entire ETL process from extraction to loading.

The pipeline performs the following steps:
1. Extract: Fetches sales data from PostgreSQL and CSV sources
2. Transform: Cleans and aggregates the data by product
3. Load: Writes the aggregated results to MySQL
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import State
from airflow.utils.email import send_email
import pandas as pd
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
    doc_md="""
    ### Extract Task
    Fetches sales data from:
    - PostgreSQL: Online sales data
    - CSV: In-store sales data
    
    Returns:
        dict: JSON strings of online and in-store sales data
    """
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
    doc_md="""
    ### Transform Task
    Processes the extracted data:
    1. Combines online and in-store sales
    2. Removes null values and invalid records
    3. Aggregates by product_id
    
    Returns:
        str: JSON string of transformed and aggregated data
    """
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
    doc_md="""
    ### Load Task
    Loads the transformed data into MySQL:
    - Inserts aggregated sales data into sales_summary table
    - Uses batch processing for better performance
    """
)

# Set task dependencies
extract_task >> transform_task >> load_task 