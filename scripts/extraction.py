"""
Extraction module for ETL pipeline.

This module handles data extraction from PostgreSQL and CSV files.
It provides functionality to:
1. Extract online sales data from PostgreSQL
2. Extract in-store sales data from CSV
3. Validate data structure and compatibility
"""

from datetime import datetime
import logging
import os
from typing import Dict, List, Optional, Tuple

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
REQUIRED_COLUMNS = ['product_id', 'quantity', 'sale_amount', 'sale_date']
NUMERIC_COLUMNS = ['product_id', 'quantity', 'sale_amount']
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
(201, 3, 60.00, %s),
(202, 2, 45.00, %s),
(203, 1, 30.00, %s),
(201, 2, 40.00, %s),
(202, 1, 22.50, %s);
"""

EXTRACT_ONLINE_SALES = """
SELECT 
    product_id::text as product_id,
    quantity::text as quantity,
    sale_amount::text as sale_amount,
    sale_date::text as sale_date
FROM online_sales
WHERE DATE(sale_date) = DATE(%s);
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
        ValueError: If validation fails
    """
    if df.empty:
        raise ValueError("DataFrame is empty")
        
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
        
    return True

def extract_online_sales(execution_date: str, pg_hook: PostgresHook) -> pd.DataFrame:
    """
    Extract online sales data from PostgreSQL for given date.
    
    Args:
        execution_date: Date to extract data for
        pg_hook: PostgreSQL connection hook
        
    Returns:
        DataFrame containing online sales data
    """
    logger.info("Extracting online sales data from PostgreSQL")
    
    sql = EXTRACT_ONLINE_SALES
    
    online_df = pg_hook.get_pandas_df(sql, parameters=[execution_date])
    logger.info(f"Extracted {len(online_df)} online sales records")
    
    validate_dataframe(online_df, REQUIRED_COLUMNS)
    return online_df

def extract_store_sales(execution_date: str) -> pd.DataFrame:
    """
    Extract in-store sales data from CSV for given date.
    
    Args:
        execution_date: Date to extract data for
        
    Returns:
        DataFrame containing in-store sales data
    """
    logger.info("Extracting in-store sales data from CSV")
    
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found at {CSV_PATH}")
            
    in_store_df = pd.read_csv(CSV_PATH)
    
    # Convert and filter by date
    in_store_df['sale_date'] = pd.to_datetime(in_store_df['sale_date']).dt.strftime('%Y-%m-%d')
    in_store_df = in_store_df[in_store_df['sale_date'] == execution_date]
    
    # Convert numeric columns to strings for consistency
    for col in NUMERIC_COLUMNS:
        in_store_df[col] = in_store_df[col].astype(str)
    
    logger.info(f"Extracted {len(in_store_df)} in-store sales records")
    validate_dataframe(in_store_df, REQUIRED_COLUMNS)
    
    return in_store_df

def setup_source_data(execution_date: str) -> None:
    """
    Set up source data tables and sample data.
    
    Args:
        execution_date: Current execution date
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Create table
        cursor.execute(CREATE_ONLINE_SALES_TABLE)
        
        # Insert sample data with current date
        cursor.execute(INSERT_SAMPLE_DATA, [execution_date] * 5)
        
        conn.commit()
        logger.info("Successfully set up source data")
        
    except Exception as e:
        logger.error(f"Failed to set up source data: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

def extract_data(**kwargs) -> Dict[str, str]:
    """
    Extract data from both sources for the execution date.
    
    Args:
        **kwargs: Airflow context variables
        
    Returns:
        dict: JSON strings of online and in-store sales data
    """
    try:
        # Get execution date
        execution_date = kwargs.get('ds', datetime.now().strftime('%Y-%m-%d'))
        logger.info(f"Extracting data for date: {execution_date}")
        
        # Extract from both sources
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        online_df = extract_online_sales(execution_date, pg_hook)
        in_store_df = extract_store_sales(execution_date)
        
        # Return data via XCom
        return {
            'online_data': online_df.to_json(date_format='iso'),
            'in_store_data': in_store_df.to_json(date_format='iso')
        }
        
    except Exception as e:
        logger.error(f"Data extraction failed: {str(e)}")
        raise

if __name__ == "__main__":
    # For testing
    test_date = datetime.now().strftime('%Y-%m-%d')
    result = extract_data(ds=test_date)
    print("Extraction test successful") 