"""
Loading module for ETL pipeline.

This module handles loading transformed sales data into PostgreSQL.
It manages table creation, data validation, and batch inserts with proper error handling.
"""

import logging
from typing import List, Tuple

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# SQL Statements
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sales_summary (
    product_id INTEGER PRIMARY KEY,
    total_quantity INTEGER NOT NULL,
    total_sale_amount DECIMAL(10,2) NOT NULL,
    sale_date DATE NOT NULL
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

def clear_existing_data(cursor, execution_date: str) -> None:
    """
    Clear existing data for the specified date.
    
    Args:
        cursor: Database cursor
        execution_date: Date to clear data for
    """
    try:
        delete_sql = "DELETE FROM sales_summary WHERE sale_date = %s"
        cursor.execute(delete_sql, (execution_date,))
        logger.info(f"Cleared existing data for date: {execution_date}")
    except Exception as e:
        logger.error(f"Failed to clear existing data: {str(e)}")
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
        logger.info("Starting load operation")
        
        # Get transformed data
        ti = kwargs['ti']
        data_json = ti.xcom_pull(task_ids='transform')
        if not data_json:
            raise ValueError("No data received from transform task")
            
        # Convert to DataFrame and validate
        df = pd.read_json(data_json)
        logger.info(f"Received DataFrame with shape: {df.shape}")
        validate_dataframe(df)
        
        # Setup database connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            # Prepare database and clear old data
            prepare_database(cursor)
            clear_existing_data(cursor, kwargs['ds'])
            
            # Insert new data
            tuples = list(df.itertuples(index=False, name=None))
            cursor.executemany(INSERT_SQL, tuples)
            
            conn.commit()
            logger.info(f"Successfully loaded {len(tuples)} records")
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Load operation failed: {str(e)}")
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
    
    load_data(ti=TestContext(), ds=test_date) 