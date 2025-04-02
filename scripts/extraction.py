"""
Extraction module for ETL pipeline.
This module handles data extraction from PostgreSQL and CSV files.

The module provides functions to:
1. Extract online sales data from PostgreSQL
2. Extract in-store sales data from CSV
3. Validate and combine the extracted data
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def get_postgres_connection():
    """
    Create connection to PostgreSQL database.
    
    Returns:
        SQLAlchemy Engine: Database connection engine
    """
    try:
        connection_string = os.getenv(
            "POSTGRES_CONNECTION_STRING",
            "postgresql://username:password@postgres:5432/sales_db"
        )
        engine = create_engine(connection_string)
        logger.info("PostgreSQL connection established successfully")
        return engine
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {str(e)}")
        raise

def validate_dataframe(df, required_columns):
    """
    Validate DataFrame has required columns and non-empty.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        bool: True if validation passes
    """
    if df.empty:
        logger.warning("DataFrame is empty")
        return False
        
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        return False
        
    return True

def validate_schemas(df_postgres, df_csv):
    """
    Validate that both dataframes have compatible schemas for merging.
    
    Args:
        df_postgres: DataFrame from PostgreSQL
        df_csv: DataFrame from CSV
        
    Returns:
        bool: True if schemas are compatible
    """
    try:
        required_columns = ['product_id', 'quantity', 'sale_amount']
        
        # Check required columns in PostgreSQL data
        postgres_columns = set(df_postgres.columns)
        if not all(col in postgres_columns for col in required_columns):
            missing = [col for col in required_columns if col not in postgres_columns]
            logger.error(f"PostgreSQL data missing required columns: {missing}")
            return False
            
        # Check required columns in CSV data
        csv_columns = set(df_csv.columns)
        if not all(col in csv_columns for col in required_columns):
            missing = [col for col in required_columns if col not in csv_columns]
            logger.error(f"CSV data missing required columns: {missing}")
            return False
            
        logger.info("Schema validation successful")
        return True
    except Exception as e:
        logger.error(f"Error validating schemas: {str(e)}")
        return False

def extract_data(**kwargs):
    """
    Extract data from PostgreSQL and CSV for the execution date.
    
    Args:
        **kwargs: Airflow context variables
        
    Returns:
        dict: JSON strings of online and in-store sales data
    """
    try:
        # 1. Extract from PostgreSQL
        logger.info("Extracting online sales data from PostgreSQL")
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        execution_date = kwargs['ds']
        
        sql = f"""
            SELECT product_id, quantity, sale_amount
            FROM online_sales
            WHERE sale_date = '{execution_date}';
        """
        online_df = pg_hook.get_pandas_df(sql)
        
        # Validate online data
        if not validate_dataframe(online_df, ['product_id', 'quantity', 'sale_amount']):
            raise ValueError("Online sales data validation failed")
            
        logger.info(f"Extracted {len(online_df)} online sales records")
        
        # 2. Extract from CSV
        logger.info("Extracting in-store sales data from CSV")
        csv_path = '/opt/airflow/data/input/in_store_sales.csv'
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at {csv_path}")
            
        in_store_df = pd.read_csv(csv_path)
        in_store_df = in_store_df[in_store_df['sale_date'] == execution_date]
        
        # Validate in-store data
        if not validate_dataframe(in_store_df, ['product_id', 'quantity', 'sale_amount']):
            raise ValueError("In-store sales data validation failed")
            
        logger.info(f"Extracted {len(in_store_df)} in-store sales records")
        
        # Validate schemas are compatible
        if not validate_schemas(online_df, in_store_df):
            raise ValueError("Schema validation failed between online and in-store data")
        
        # Return data via XCom (small datasets only!)
        return {
            'online_data': online_df.to_json(),
            'in_store_data': in_store_df.to_json()
        }
        
    except Exception as e:
        logger.error(f"Data extraction failed: {str(e)}")
        raise

if __name__ == "__main__":
    # For testing
    try:
        # Create a mock execution date for testing
        test_date = datetime.now().strftime('%Y-%m-%d')
        result = extract_data(ds=test_date)
        print("Extraction test successful")
        print(f"Online data records: {len(pd.read_json(result['online_data']))}")
        print(f"In-store data records: {len(pd.read_json(result['in_store_data']))}")
    except Exception as e:
        print(f"Error: {str(e)}") 