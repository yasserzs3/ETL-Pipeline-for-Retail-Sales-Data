"""
ETL Pipeline DAG for Sales Data Processing.
This DAG orchestrates the entire ETL process from extraction to loading.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Add the scripts directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts'))

# Import the ETL modules
from extraction import extract
from transformation import transform
from loading import load

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# Instantiate the DAG
dag = DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for processing sales data',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False,
    tags=['etl', 'sales'],
)

# Define task functions
def extract_task():
    """
    Task to extract data from sources.
    
    Returns:
        tuple: (PostgreSQL DataFrame, CSV DataFrame)
    """
    try:
        logger.info("Starting data extraction")
        df_postgres, df_csv = extract()
        logger.info(f"Extraction complete: {len(df_postgres)} PostgreSQL records, {len(df_csv)} CSV records")
        return df_postgres.to_json(), df_csv.to_json()
    except Exception as e:
        logger.error(f"Extraction task failed: {str(e)}")
        raise

def transform_task(ti):
    """
    Task to transform extracted data.
    
    Args:
        ti: Task instance from which to pull the extracted data
        
    Returns:
        tuple: (Merged DataFrame JSON, Dict of aggregation file paths)
    """
    try:
        logger.info("Starting data transformation")
        
        # Get the extracted data from XCom
        df_postgres_json, df_csv_json = ti.xcom_pull(task_ids='extract_data')
        
        # Convert JSON strings back to DataFrames
        df_postgres = pd.read_json(df_postgres_json)
        df_csv = pd.read_json(df_csv_json)
        
        # Perform transformation
        df_merged, aggregations, paths = transform(df_postgres, df_csv)
        
        logger.info(f"Transformation complete: {len(df_merged)} merged records")
        
        # Return the merged data and file paths for the load task
        return df_merged.to_json(), paths
    except Exception as e:
        logger.error(f"Transformation task failed: {str(e)}")
        raise

def load_task(ti):
    """
    Task to load transformed data.
    
    Args:
        ti: Task instance from which to pull the transformed data
    """
    try:
        logger.info("Starting data loading")
        
        # Get the transformed data from XCom
        df_merged_json, paths = ti.xcom_pull(task_ids='transform_data')
        
        # Load the transformed data
        # For this task, we'll read the aggregated data from the saved files
        # rather than passing all the DataFrames through XCom
        
        # Define the directory where aggregations were saved
        output_dir = os.path.join('data', 'output')
        
        # Dictionary to store the loaded aggregations
        aggregations = {}
        
        # Load each aggregation from files
        for agg_name, file_path in paths['aggregations'].items():
            if os.path.exists(file_path):
                aggregations[agg_name] = pd.read_csv(file_path)
                logger.info(f"Loaded {agg_name} from {file_path}: {len(aggregations[agg_name])} records")
        
        # Load data into MySQL
        results = load(aggregations)
        
        logger.info(f"Loading complete: {results}")
        return results
    except Exception as e:
        logger.error(f"Loading task failed: {str(e)}")
        raise

# Define the tasks
extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_task,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    dag=dag,
)

# Set task dependencies
extract_data >> transform_data >> load_data 