"""
ETL Pipeline DAG for Sales Data Processing.
This DAG orchestrates the entire ETL process from extraction to loading.

The pipeline performs the following steps:
1. Extract: Fetches sales data from PostgreSQL and CSV sources
2. Transform: Cleans and aggregates the data by product
3. Load: Writes the aggregated results to PostgreSQL
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import State
from airflow.utils.email import send_email
from airflow.utils.log.logging_mixin import LoggingMixin
import pandas as pd
from scripts.extraction import extract_data
from scripts.transformation import transform_data
from scripts.loading import load_data

# Initialize logger using Airflow's LoggingMixin
logger = LoggingMixin().log

def task_start_handler(context):
    """Handler called when a task starts execution."""
    task_instance: TaskInstance = context['task_instance']
    task_instance.xcom_push(key='start_time', value=datetime.now().isoformat())
    task_instance.xcom_push(key='task_status', value='STARTED')
    logger.info(f"Task {task_instance.task_id} started execution")

def task_success_handler(context):
    """Handler called when a task completes successfully."""
    task_instance: TaskInstance = context['task_instance']
    execution_date = context['execution_date']
    
    # Get start time from XCom
    start_time_str = task_instance.xcom_pull(key='start_time', task_ids=task_instance.task_id)
    if start_time_str:
        start_time = datetime.fromisoformat(start_time_str)
        duration = (datetime.now() - start_time).total_seconds()
    else:
        duration = task_instance.duration
    
    # Log success event and details
    task_instance.xcom_push(key='task_status', value='SUCCESS')
    task_instance.xcom_push(key='end_time', value=datetime.now().isoformat())
    task_instance.xcom_push(key='duration_seconds', value=duration)
    
    # Record metrics based on task
    if task_instance.task_id == 'extract':
        data = task_instance.xcom_pull(task_ids='extract')
        if data:
            task_instance.xcom_push(key='records_extracted', value={
                'online': len(pd.read_json(data['online_data'])),
                'in_store': len(pd.read_json(data['in_store_data']))
            })
    elif task_instance.task_id == 'transform':
        data = task_instance.xcom_pull(task_ids='transform')
        if data:
            task_instance.xcom_push(key='records_transformed', value=len(pd.read_json(data)))
    elif task_instance.task_id == 'load':
        # Metrics are logged inside the loading function
        pass
    
    logger.info(f"Task {task_instance.task_id} completed successfully in {duration:.2f} seconds")

def task_failure_handler(context):
    """Handler called when a task fails."""
    task_instance: TaskInstance = context['task_instance']
    execution_date = context['execution_date']
    exception = context.get('exception')
    
    # Log failure event
    task_instance.xcom_push(key='task_status', value='FAILED')
    task_instance.xcom_push(key='end_time', value=datetime.now().isoformat())
    task_instance.xcom_push(key='error_message', value=str(exception))
    
    # Send email notification for critical failures
    subject = f"Airflow Alert: Task {task_instance.task_id} in DAG {task_instance.dag_id} failed"
    body = f"""
    Task: {task_instance.task_id}
    DAG: {task_instance.dag_id}
    Execution Date: {execution_date}
    Error: {exception}
    """
    send_email(['admin@example.com'], subject, body)
    
    logger.error(f"Task {task_instance.task_id} failed: {exception}")

def dag_success_callback(context):
    """Handler called when the DAG completes successfully."""
    dag_run = context['dag_run']
    logger.info(f"DAG {dag_run.dag_id} completed successfully at {datetime.now().isoformat()}")
    
    # Calculate and log overall ETL pipeline metrics
    task_instances = dag_run.get_task_instances()
    total_duration = sum(ti.duration or 0 for ti in task_instances)
    
    # Store overall metrics
    for ti in task_instances:
        if ti.task_id == 'load':
            ti.xcom_push(key='etl_total_duration', value=total_duration)
            ti.xcom_push(key='etl_completion_time', value=datetime.now().isoformat())

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 1),
    'on_failure_callback': task_failure_handler,
    'on_success_callback': task_success_handler,
    'on_retry_callback': task_failure_handler,
}

# Log DAG initialization
logger.info("Initializing retail_sales_etl DAG")

# Instantiate the DAG
dag = DAG(
    'retail_sales_etl',
    default_args=default_args,
    description='ETL pipeline for processing sales data',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'sales'],
    on_success_callback=dag_success_callback,
)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
    on_execute_callback=task_start_handler,
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
    on_execute_callback=task_start_handler,
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
    on_execute_callback=task_start_handler,
    doc_md="""
    ### Load Task
    Loads the transformed data into PostgreSQL:
    - Inserts aggregated sales data into sales_summary table
    - Uses batch processing for better performance
    """
)

# Log task dependencies
logger.info("Setting up task dependencies")

# Set task dependencies
extract_task >> transform_task >> load_task 