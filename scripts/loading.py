"""
Loading module for ETL pipeline.

This module handles loading transformed sales data into PostgreSQL.
It manages table creation, data validation, and batch inserts with proper error handling.
"""

from typing import List, Tuple
import time

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

# Initialize logger
logger = LoggingMixin().log

# SQL Statements
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sales_summary (
    product_id INTEGER PRIMARY KEY,
    sale_date DATE NOT NULL,
    total_quantity INTEGER NOT NULL,
    total_sale_amount DECIMAL(10,2) NOT NULL
);
"""

INSERT_SQL = """
INSERT INTO sales_summary 
(product_id, total_quantity, total_sale_amount, sale_date)
VALUES (%s, %s, %s, %s)
ON CONFLICT (product_id) 
DO UPDATE SET 
    total_quantity = EXCLUDED.total_quantity,
    total_sale_amount = EXCLUDED.total_sale_amount,
    sale_date = EXCLUDED.sale_date;
"""

def validate_dataframe(df: pd.DataFrame) -> None:
    """
    Validate DataFrame structure and content.
    
    Args:
        df: DataFrame to validate
        
    Raises:
        ValueError: If validation fails
    """
    required_columns = ['product_id', 'total_quantity', 'total_sale_amount', 'sale_date']
    
    if df.empty:
        raise ValueError("DataFrame is empty")
        
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
        
    if df['total_quantity'].lt(0).any():
        raise ValueError("Found negative quantities")
        
    if df['total_sale_amount'].lt(0).any():
        raise ValueError("Found negative sale amounts")

def prepare_database(cursor) -> None:
    """
    Prepare database by creating necessary tables.
    
    Args:
        cursor: Database cursor
    """
    try:
        cursor.execute(CREATE_TABLE_SQL)
        logger.info("Table sales_summary created or verified")
    except Exception as e:
        logger.error(f"Failed to create table: {str(e)}")
        raise

def load_data(**kwargs) -> None:
    """
    Load transformed data into PostgreSQL sales_summary table.
    
    Args:
        **kwargs: Airflow context variables
        
    Raises:
        ValueError: If data loading fails
    """
    try:
        # Start timing the operation
        start_time = time.time()
        
        # Get task instance for XCom
        ti = kwargs['ti']
        
        # Log load operation started
        ti.xcom_push(key='load_operation_status', value='STARTED')
        logger.info("Starting load operation")
        
        # Get transformed data
        data_json = ti.xcom_pull(task_ids='transform')
        if not data_json:
            error_msg = "No data received from transform task"
            ti.xcom_push(key='load_error', value=error_msg)
            raise ValueError(error_msg)
            
        # Convert to DataFrame and validate
        df = pd.read_json(data_json)
        record_count = len(df)
        ti.xcom_push(key='load_records_count', value=record_count)
        logger.info(f"Received DataFrame with shape: {df.shape}")
        logger.info(f"DataFrame columns: {df.columns.tolist()}")
        logger.info(f"First 5 records: \n{df.head().to_string()}")
        
        # Log validation start
        validation_start = time.time()
        ti.xcom_push(key='validation_status', value='STARTED')
        
        validate_dataframe(df)
        
        # Log validation completion
        validation_time = time.time() - validation_start
        ti.xcom_push(key='validation_status', value='COMPLETED')
        ti.xcom_push(key='validation_time_seconds', value=validation_time)
        
        # Ensure proper data types
        df['product_id'] = df['product_id'].astype(int)
        df['total_quantity'] = df['total_quantity'].astype(int)
        df['total_sale_amount'] = df['total_sale_amount'].astype(float)
        
        # Setup database connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Log database operation metrics
        db_start_time = time.time()
        ti.xcom_push(key='db_operation_status', value='STARTED')
        
        try:
            # Prepare database
            prepare_database(cursor)
            
            # Insert new data
            tuples = list(df.itertuples(index=False, name=None))
            insert_start_time = time.time()
            ti.xcom_push(key='insert_operation_status', value='STARTED')
            logger.info(f"Preparing to insert {len(tuples)} records")
            
            cursor.executemany(INSERT_SQL, tuples)
            
            # Log insert completion
            insert_time = time.time() - insert_start_time
            ti.xcom_push(key='insert_operation_status', value='COMPLETED')
            ti.xcom_push(key='insert_time_seconds', value=insert_time)
            ti.xcom_push(key='inserted_record_count', value=len(tuples))
            
            conn.commit()
            logger.info(f"Successfully loaded {len(tuples)} records")
            
            # Log database operation completion
            db_total_time = time.time() - db_start_time
            ti.xcom_push(key='db_operation_status', value='COMPLETED')
            ti.xcom_push(key='db_operation_time_seconds', value=db_total_time)
            
        except Exception as e:
            conn.rollback()
            error_msg = f"Database error: {str(e)}"
            ti.xcom_push(key='db_operation_status', value='FAILED')
            ti.xcom_push(key='db_error', value=error_msg)
            logger.error(error_msg)
            raise e
        finally:
            cursor.close()
            conn.close()
            
        # Log overall load operation completion
        total_time = time.time() - start_time
        ti.xcom_push(key='load_operation_status', value='COMPLETED')
        ti.xcom_push(key='load_operation_time_seconds', value=total_time)
        logger.info(f"Load operation completed in {total_time:.2f} seconds")
            
    except Exception as e:
        error_msg = f"Load operation failed: {str(e)}"
        if 'ti' in locals():
            ti.xcom_push(key='load_operation_status', value='FAILED')
            ti.xcom_push(key='load_error', value=error_msg)
        logger.error(error_msg)
        raise

if __name__ == "__main__":
    # For testing
    import json
    from datetime import datetime
    
    test_date = datetime.now().strftime('%Y-%m-%d')
    test_data = pd.DataFrame({
        'product_id': [1, 2],
        'total_quantity': [3, 4],
        'total_sale_amount': [30.0, 40.0],
        'sale_date': [test_date, test_date]
    })
    
    class TestContext:
        def xcom_pull(self, task_ids):
            return test_data.to_json()
            
        def xcom_push(self, key, value):
            print(f"XCom push: {key} = {value}")
    
    load_data(ti=TestContext(), ds=test_date) 