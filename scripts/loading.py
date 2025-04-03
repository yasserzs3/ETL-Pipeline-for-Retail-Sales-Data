"""
Loading module for ETL pipeline.

This module handles loading transformed and pre-aggregated sales data into PostgreSQL.
It receives fully aggregated data from the transformation step and does not perform
any additional aggregation. The module handles table creation, data validation,
CSV file output, and database loading with proper error handling.
"""

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import logging

# SQL Statements
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sales_summary (
    product_id INTEGER PRIMARY KEY,
    total_quantity INTEGER NOT NULL,
    total_sale_amount DECIMAL(10,2) NOT NULL
);
"""

# SQL to replace existing records on conflict
INSERT_SQL = """
INSERT INTO sales_summary 
(product_id, total_quantity, total_sale_amount)
VALUES (%s, %s, %s)
ON CONFLICT (product_id) 
DO UPDATE SET 
    total_quantity = EXCLUDED.total_quantity,
    total_sale_amount = EXCLUDED.total_sale_amount;
"""

# SQL to truncate the table
TRUNCATE_TABLE_SQL = """
TRUNCATE TABLE sales_summary;
"""

def validate_dataframe(df: pd.DataFrame) -> None:
    """
    Validate DataFrame structure and content.
    
    Checks that the DataFrame:
    1. Is not empty
    2. Contains all required columns
    3. Does not contain negative quantities or sale amounts
    
    Args:
        df: DataFrame to validate with columns 'product_id', 'total_quantity', 'total_sale_amount'
        
    Raises:
        ValueError: If DataFrame is empty, missing columns, or contains negative values
    """
    logger = logging.getLogger(__name__)
    
    required_columns = ['product_id', 'total_quantity', 'total_sale_amount']
    
    if df.empty:
        logger.error("DataFrame is empty")
        raise ValueError("DataFrame is empty")
        
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")
        
    if df['total_quantity'].lt(0).any():
        logger.error("Found negative quantities")
        raise ValueError("Found negative quantities")
        
    if df['total_sale_amount'].lt(0).any():
        logger.error("Found negative sale amounts")
        raise ValueError("Found negative sale amounts")
    
    logger.info("DataFrame validation passed")

def load_data(**kwargs) -> None:
    """
    Load pre-aggregated data into PostgreSQL sales_summary table and save to CSV.
    
    Main loading function that:
    1. Retrieves transformed and already aggregated data from the transform task via XCom
    2. Validates the data structure and content
    3. Saves to CSV file in the output directory (overwrites existing file)
    4. Loads into PostgreSQL, replacing any existing records
    
    Args:
        **kwargs: Airflow context variables, including task_instance
        
    Raises:
        ValueError: If data is not received from transform task or validation fails
        Exception: If database operations fail
    """
    # Set up logger
    logger = logging.getLogger(__name__)
    logger.info("Starting data loading process")
    
    try:
        # Get task instance for XCom
        ti = kwargs['ti']
        
        # Get transformed data
        data_json = ti.xcom_pull(task_ids='transform')
        if not data_json:
            error_msg = "No data received from transform task"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        # Convert to DataFrame and validate
        df = pd.read_json(data_json)
        logger.info(f"Received {len(df)} rows from transform step")
        
        # No need to aggregate again - data is already aggregated by product_id in the transform step
        
        # Validate data
        validate_dataframe(df)
        
        # Ensure proper data types
        df['product_id'] = df['product_id'].astype(int)
        df['total_quantity'] = df['total_quantity'].astype(int)
        df['total_sale_amount'] = df['total_sale_amount'].astype(float)
        logger.info("Data types converted successfully")
        
        # Save to CSV in data/output directory
        # This directory is mounted in the Airflow container
        output_dir = '/opt/airflow/data/output'
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'sales_summary.csv')
        
        # Always overwrite the CSV file with new data
        df.to_csv(output_file, index=False)
        logger.info(f"Data saved to CSV file: {output_file}")
        
        # Setup database connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            # Create table if it doesn't exist
            cursor.execute(CREATE_TABLE_SQL)
            logger.info("Created sales_summary table if it didn't exist")
            
            # Truncate the table to remove all existing data
            cursor.execute(TRUNCATE_TABLE_SQL)
            logger.info("Truncated sales_summary table")
            
            # Use executemany for more efficient batch insert
            records = [
                (
                    int(row['product_id']), 
                    int(row['total_quantity']), 
                    float(row['total_sale_amount'])
                ) 
                for _, row in df.iterrows()
            ]
            
            if records:
                cursor.executemany(INSERT_SQL, records)
                conn.commit()
                logger.info(f"Successfully inserted {len(records)} rows in database")
            else:
                logger.warning("No records to insert in database")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {str(e)}")
            raise e
        finally:
            cursor.close()
            conn.close()
            logger.info("Database connection closed")
            
    except Exception as e:
        logger.error(f"Load error: {str(e)}")
        raise 