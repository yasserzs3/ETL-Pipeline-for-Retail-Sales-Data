"""
Loading module for ETL pipeline.
This module handles loading data into a MySQL data warehouse.
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, Date, MetaData
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize SQLAlchemy components
Base = declarative_base()
metadata = MetaData()

# Define tables
product_agg_table = Table(
    'product_aggregation', 
    metadata,
    Column('id', Integer, primary_key=True),
    Column('product_id', Integer, nullable=False),
    Column('total_quantity', Integer, nullable=False),
    Column('total_revenue', Float, nullable=False),
    Column('avg_price', Float, nullable=False),
    Column('transaction_count', Integer, nullable=False),
)

product_source_agg_table = Table(
    'product_source_aggregation', 
    metadata,
    Column('id', Integer, primary_key=True),
    Column('product_id', Integer, nullable=False),
    Column('source', String(50), nullable=False),
    Column('total_quantity', Integer, nullable=False),
    Column('total_revenue', Float, nullable=False),
)

date_agg_table = Table(
    'date_aggregation', 
    metadata,
    Column('id', Integer, primary_key=True),
    Column('date', Date, nullable=False),
    Column('total_quantity', Integer, nullable=False),
    Column('total_revenue', Float, nullable=False),
    Column('product_count', Integer, nullable=False),
)

product_date_agg_table = Table(
    'product_date_aggregation', 
    metadata,
    Column('id', Integer, primary_key=True),
    Column('product_id', Integer, nullable=False),
    Column('date', Date, nullable=False),
    Column('total_quantity', Integer, nullable=False),
    Column('total_revenue', Float, nullable=False),
)

store_agg_table = Table(
    'store_aggregation', 
    metadata,
    Column('id', Integer, primary_key=True),
    Column('store_id', String(50), nullable=False),
    Column('total_quantity', Integer, nullable=False),
    Column('total_revenue', Float, nullable=False),
    Column('product_count', Integer, nullable=False),
)

def get_mysql_connection():
    """
    Create connection to MySQL database.
    
    Returns:
        SQLAlchemy Engine: Database connection engine
    """
    try:
        connection_string = os.getenv(
            "MYSQL_CONNECTION_STRING",
            "mysql+pymysql://username:password@mysql:3306/sales_warehouse"
        )
        engine = create_engine(connection_string)
        logger.info("MySQL connection established successfully")
        return engine
    except Exception as e:
        logger.error(f"Error connecting to MySQL: {str(e)}")
        raise

def create_tables(engine):
    """
    Create database tables if they don't exist.
    
    Args:
        engine: SQLAlchemy engine for database connection
    """
    try:
        metadata.create_all(engine)
        logger.info("Tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise

def load_dataframe(df, table, engine, batch_size=1000):
    """
    Load a DataFrame into a database table with batch processing.
    
    Args:
        df: DataFrame to load
        table: SQLAlchemy Table object
        engine: SQLAlchemy engine for database connection
        batch_size: Number of records to insert in each batch
        
    Returns:
        int: Number of records loaded
    """
    try:
        # Convert DataFrame to records
        records = df.to_dict(orient='records')
        total_records = len(records)
        
        # Process in batches
        with engine.connect() as connection:
            for i in range(0, total_records, batch_size):
                batch = records[i:i+batch_size]
                if batch:
                    connection.execute(table.insert(), batch)
                    logger.info(f"Loaded batch {i//batch_size + 1}/{(total_records-1)//batch_size + 1} ({len(batch)} records)")
        
        logger.info(f"Successfully loaded {total_records} records into {table.name}")
        return total_records
    except Exception as e:
        logger.error(f"Error loading data into {table.name}: {str(e)}")
        raise

def load_aggregations(aggregations, engine):
    """
    Load all aggregated data into respective tables.
    
    Args:
        aggregations: Dictionary of aggregated DataFrames
        engine: SQLAlchemy engine for database connection
        
    Returns:
        dict: Summary of loaded records per table
    """
    try:
        results = {}
        
        # Load product aggregation
        if 'product_agg' in aggregations and aggregations['product_agg'] is not None:
            count = load_dataframe(aggregations['product_agg'], product_agg_table, engine)
            results['product_aggregation'] = count
            
        # Load product source aggregation
        if 'product_source_agg' in aggregations and aggregations['product_source_agg'] is not None:
            count = load_dataframe(aggregations['product_source_agg'], product_source_agg_table, engine)
            results['product_source_aggregation'] = count
            
        # Load date aggregation
        if 'date_agg' in aggregations and aggregations['date_agg'] is not None:
            count = load_dataframe(aggregations['date_agg'], date_agg_table, engine)
            results['date_aggregation'] = count
            
        # Load product date aggregation
        if 'product_date_agg' in aggregations and aggregations['product_date_agg'] is not None:
            count = load_dataframe(aggregations['product_date_agg'], product_date_agg_table, engine)
            results['product_date_aggregation'] = count
            
        # Load store aggregation
        if 'store_agg' in aggregations and aggregations['store_agg'] is not None:
            count = load_dataframe(aggregations['store_agg'], store_agg_table, engine)
            results['store_aggregation'] = count
            
        logger.info(f"All aggregations loaded successfully: {results}")
        return results
    except Exception as e:
        logger.error(f"Error loading aggregations: {str(e)}")
        raise

def load(aggregations):
    """
    Main loading function.
    
    Args:
        aggregations: Dictionary of aggregated DataFrames
        
    Returns:
        dict: Summary of loaded records per table
    """
    try:
        # Get database connection
        engine = get_mysql_connection()
        
        # Create tables if they don't exist
        create_tables(engine)
        
        # Load aggregations
        results = load_aggregations(aggregations, engine)
        
        return results
    except Exception as e:
        logger.error(f"Loading process failed: {str(e)}")
        raise

if __name__ == "__main__":
    # For testing
    try:
        # Import previous modules
        from extraction import extract
        from transformation import transform
        
        # Extract and transform data
        df_online, df_instore = extract()
        merged, aggregations, paths = transform(df_online, df_instore)
        
        # Load data
        results = load(aggregations)
        print(f"Loading results: {results}")
    except Exception as e:
        print(f"Error: {str(e)}") 