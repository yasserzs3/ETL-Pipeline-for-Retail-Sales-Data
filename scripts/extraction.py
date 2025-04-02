"""
Extraction module for ETL pipeline.
This module handles data extraction from PostgreSQL and CSV files.
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

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

def extract_from_postgres(engine, query=None):
    """
    Extract data from PostgreSQL database.
    
    Args:
        engine: SQLAlchemy engine for database connection
        query: SQL query string to execute (default: query online_sales table)
        
    Returns:
        pandas.DataFrame: Extracted data
    """
    try:
        if query is None:
            query = """
            SELECT 
                product_id, 
                date, 
                quantity, 
                price, 
                quantity * price as total_amount 
            FROM online_sales
            """
        
        df = pd.read_sql_query(query, engine)
        logger.info(f"Successfully extracted {len(df)} records from PostgreSQL")
        return df
    except Exception as e:
        logger.error(f"Error extracting data from PostgreSQL: {str(e)}")
        raise

def extract_from_csv(file_path=None):
    """
    Extract data from a CSV file.
    
    Args:
        file_path: Path to the CSV file (default: data/input/in_store_sales.csv)
        
    Returns:
        pandas.DataFrame: Extracted data
    """
    try:
        if file_path is None:
            file_path = os.path.join('data', 'input', 'in_store_sales.csv')
        
        if not os.path.exists(file_path):
            error_msg = f"CSV file not found at {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
            
        df = pd.read_csv(file_path)
        logger.info(f"Successfully extracted {len(df)} records from CSV at {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error extracting data from CSV: {str(e)}")
        raise

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
        required_columns = ['product_id', 'date', 'quantity', 'price', 'total_amount']
        
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

def extract():
    """
    Main function to extract data from all sources.
    
    Returns:
        tuple: (PostgreSQL DataFrame, CSV DataFrame)
    """
    try:
        # Extract from PostgreSQL
        engine = get_postgres_connection()
        df_postgres = extract_from_postgres(engine)
        
        # Extract from CSV
        df_csv = extract_from_csv()
        
        # Validate schemas
        if not validate_schemas(df_postgres, df_csv):
            raise ValueError("Schema validation failed")
            
        return df_postgres, df_csv
    except Exception as e:
        logger.error(f"Extraction process failed: {str(e)}")
        raise

if __name__ == "__main__":
    # For testing
    try:
        online_sales, in_store_sales = extract()
        print(f"Online sales shape: {online_sales.shape}")
        print(f"In-store sales shape: {in_store_sales.shape}")
    except Exception as e:
        print(f"Error: {str(e)}") 