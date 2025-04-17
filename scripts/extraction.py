"""
Extraction module for ETL pipeline.

This module handles data extraction from PostgreSQL and CSV files.
Note: While extraction uses PostgreSQL, the loading phase uses MySQL.

It provides functionality to:
1. Extract online sales data from PostgreSQL
2. Extract in-store sales data from CSV
3. Validate data structure and compatibility
"""

from datetime import datetime
import os
import logging
import traceback
from typing import Dict, List, Optional, Tuple

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Constants
REQUIRED_COLUMNS = ['sale_id', 'product_id', 'quantity', 'sale_amount', 'sale_date']
NUMERIC_COLUMNS = ['sale_id', 'product_id', 'quantity', 'sale_amount']
CSV_PATH = '/opt/airflow/data/input/in_store_sales.csv'

# SQL Statements
CREATE_ONLINE_SALES_TABLE = """
CREATE TABLE IF NOT EXISTS online_sales (
    sale_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    quantity INTEGER,
    sale_amount DECIMAL(10,2),
    sale_date DATE
);
"""

INSERT_SAMPLE_DATA = """
INSERT INTO online_sales (product_id, quantity, sale_amount, sale_date) VALUES
-- First day data with valid entries
(101, 3, 60.00, %s),
(102, 2, 45.00, %s),
(103, 1, 30.00, %s),
(104, 2, 40.00, %s),
(105, 1, 22.50, %s),
-- Second day data (using the day after execution date)
(101, 4, 120.00, date(%s) + interval '1 day'),
(102, 2, 55.00, date(%s) + interval '1 day'),
(103, 3, 75.00, date(%s) + interval '1 day'),
(105, 1, 20.00, date(%s) + interval '1 day'),
(106, 2, 55.00, date(%s) + interval '1 day'),
-- Test data with invalid values for cleaning
(101, 0, 10.00, date(%s) + interval '1 day'),
(102, -1, 20.00, date(%s) + interval '1 day'),
(103, 2, -5.00, date(%s) + interval '1 day'),
(104, NULL, 30.00, date(%s) + interval '1 day'),
(105, 2, NULL, date(%s) + interval '1 day');
"""

EXTRACT_ONLINE_SALES = """
SELECT 
    sale_id::text as sale_id,
    product_id::text as product_id,
    quantity::text as quantity,
    sale_amount::text as sale_amount,
    sale_date::text as sale_date
FROM online_sales;
"""

def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    Validate DataFrame has required columns and is non-empty.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        bool: True if validation passes
        
    Raises:
        ValueError: If DataFrame is empty or missing required columns
    """
    logger = logging.getLogger(__name__)
    
    if df.empty:
        logger.error("DataFrame is empty")
        raise ValueError("DataFrame is empty")
        
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")
        
    logger.info(f"Validation passed for columns: {required_columns}")
    return True

def extract_online_sales(pg_hook: PostgresHook) -> pd.DataFrame:
    """
    Extract all online sales data from PostgreSQL.
    
    Retrieves sales data from the online_sales table, converting all columns
    to string format for consistency. Creates the table with sample data if
    it doesn't exist.
    
    Args:
        pg_hook: PostgreSQL connection hook
        
    Returns:
        DataFrame containing online sales data with string-type columns
        
    Raises:
        Exception: If database connection or query fails
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting extraction of online sales data")
    
    # Check if table exists
    logger.info("Checking if online_sales table exists")
    table_exists = False
    try:
        check_table_sql = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'online_sales');"
        table_exists = pg_hook.get_first(check_table_sql)[0]
    except Exception as e:
        logger.warning(f"Error checking table existence: {str(e)}")
    
    # Create table and insert sample data if it doesn't exist
    if not table_exists:
        logger.info("online_sales table does not exist, creating it with sample data")
        # Use today's date since we're creating the table for the first time
        execution_date = datetime.now().strftime('%Y-%m-%d')
        setup_source_data(execution_date)
        logger.info("Created online_sales table with sample data")
    
    sql = EXTRACT_ONLINE_SALES
    logger.info(f"Executing SQL: {sql}")
    
    online_df = pg_hook.get_pandas_df(sql)
    logger.info(f"Retrieved {len(online_df)} rows from PostgreSQL")
    
    validate_dataframe(online_df, REQUIRED_COLUMNS)
    logger.info(f"Sample online data: {online_df.head().to_dict('records')}")
    
    return online_df

def extract_store_sales() -> pd.DataFrame:
    """
    Extract in-store sales data from CSV.
    
    Reads in-store sales data from a CSV file, converts the date format to 
    'YYYY-MM-DD' for consistency, and converts numeric columns to strings
    to match the format of online sales data.
    
    Returns:
        DataFrame containing in-store sales data with formatted dates and string-type columns
        
    Raises:
        FileNotFoundError: If CSV file does not exist
        ValueError: If validation fails
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting extraction of in-store sales data from {CSV_PATH}")
    
    if not os.path.exists(CSV_PATH):
        logger.error(f"CSV file not found at {CSV_PATH}")
        raise FileNotFoundError(f"CSV file not found at {CSV_PATH}")
            
    in_store_df = pd.read_csv(CSV_PATH)
    logger.info(f"Read {len(in_store_df)} rows from CSV file")
    
    # Convert date format for consistency but don't filter by date
    logger.info("Converting date format")
    in_store_df['sale_date'] = pd.to_datetime(in_store_df['sale_date']).dt.strftime('%Y-%m-%d')
    
    # Convert numeric columns to strings for consistency
    logger.info(f"Converting numeric columns to strings: {NUMERIC_COLUMNS}")
    for col in NUMERIC_COLUMNS:
        in_store_df[col] = in_store_df[col].astype(str)
    
    validate_dataframe(in_store_df, REQUIRED_COLUMNS)
    logger.info(f"Sample in-store data: {in_store_df.head().to_dict('records')}")
    
    return in_store_df

def setup_source_data(execution_date: str) -> None:
    """
    Set up source data tables and sample data.
    
    Creates the online_sales table if it doesn't exist and populates it
    with sample data only if the table is empty.
    
    Args:
        execution_date: Current execution date used to timestamp sample data
        
    Raises:
        Exception: If database operations fail
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Setting up source data for execution date: {execution_date}")
    
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Create table
        logger.info("Creating online_sales table if not exists")
        cursor.execute(CREATE_ONLINE_SALES_TABLE)
        
        # Check if table has data
        cursor.execute("SELECT COUNT(*) FROM online_sales")
        row_count = cursor.fetchone()[0]
        
        if row_count > 0:
            logger.info(f"Table online_sales already has {row_count} rows, skipping sample data insertion")
        else:
            # Count the number of placeholders in the SQL statement
            placeholder_count = INSERT_SAMPLE_DATA.count('%s')
            logger.info(f"Detected {placeholder_count} placeholders in sample data SQL")
            
            # Create a parameter list with the appropriate length
            params = [execution_date] * placeholder_count
            
            # Insert sample data with current date
            logger.info(f"Inserting sample data with {placeholder_count} parameters")
            cursor.execute(INSERT_SAMPLE_DATA, params)
            logger.info("Sample data insertion complete")
        
        conn.commit()
        logger.info("Sample data setup complete")
        
    except Exception as e:
        logger.error(f"Error setting up source data: {str(e)}")
        logger.error(traceback.format_exc())
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()
            logger.info("Database connection closed")

def extract_data(**kwargs) -> Dict[str, str]:
    """
    Extract data from both online and in-store sources.
    
    Main entry point for the extraction task. Orchestrates the extraction of data
    from PostgreSQL (online sales) and CSV files (in-store sales), ensuring the
    database tables exist with sample data if needed.
    
    Args:
        **kwargs: Airflow context variables, including 'ds' (execution date)
        
    Returns:
        dict: Dictionary with 'online_data' and 'in_store_data' keys containing
              JSON string representations of the respective DataFrames
        
    Raises:
        Exception: If extraction from either source fails
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting data extraction")
    
    try:
        # Get execution date
        execution_date = kwargs.get('ds', datetime.now().strftime('%Y-%m-%d'))
        logger.info(f"Execution date: {execution_date}")
        
        # Extract from both sources
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        
        # Make sure database is set up - will only create if not exists
        try:
            logger.info("Ensuring database tables are set up")
            setup_source_data(execution_date)
        except Exception as e:
            logger.warning(f"Non-critical error during database setup: {str(e)}")
            logger.warning("Continuing with extraction as tables might already exist")
        
        logger.info("Extracting online sales data")
        online_df = extract_online_sales(pg_hook)
        logger.info(f"Extracted {len(online_df)} rows of online sales data")
        
        logger.info("Extracting in-store sales data")
        in_store_df = extract_store_sales()
        logger.info(f"Extracted {len(in_store_df)} rows of in-store sales data")
        
        # Return data via XCom
        logger.info("Preparing data for XCom transfer")
        result = {
            'online_data': online_df.to_json(date_format='iso'),
            'in_store_data': in_store_df.to_json(date_format='iso')
        }
        logger.info("Data extraction complete")
        return result
        
    except Exception as e:
        logger.error(f"Extraction error: {str(e)}")
        logger.error(traceback.format_exc())
        raise 