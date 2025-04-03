"""
Loading module for ETL pipeline.

This module handles loading transformed sales data into PostgreSQL.
It manages table creation, data validation, and batch inserts with proper error handling.
"""

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    except Exception as e:
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
        # Get task instance for XCom
        ti = kwargs['ti']
        
        # Get transformed data
        data_json = ti.xcom_pull(task_ids='transform')
        if not data_json:
            error_msg = "No data received from transform task"
            raise ValueError(error_msg)
            
        # Convert to DataFrame and validate
        df = pd.read_json(data_json)
        validate_dataframe(df)
        
        # Ensure proper data types
        df['product_id'] = df['product_id'].astype(int)
        df['total_quantity'] = df['total_quantity'].astype(int)
        df['total_sale_amount'] = df['total_sale_amount'].astype(float)
        
        # Setup database connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            # Prepare database
            prepare_database(cursor)
            
            # Insert new data
            tuples = list(df.itertuples(index=False, name=None))
            cursor.executemany(INSERT_SQL, tuples)
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        raise 