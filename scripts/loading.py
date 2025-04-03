"""
Loading module for ETL pipeline.

This module handles loading transformed sales data into PostgreSQL.
It manages table creation, data validation, and batch inserts with proper error handling.
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

INSERT_SQL = """
INSERT INTO sales_summary 
(product_id, total_quantity, total_sale_amount)
VALUES (%s, %s, %s)
ON CONFLICT (product_id) 
DO UPDATE SET 
    total_quantity = sales_summary.total_quantity + EXCLUDED.total_quantity,
    total_sale_amount = sales_summary.total_sale_amount + EXCLUDED.total_sale_amount;
"""

def validate_dataframe(df: pd.DataFrame) -> None:
    """
    Validate DataFrame structure and content.
    
    Args:
        df: DataFrame to validate
        
    Raises:
        ValueError: If validation fails
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
    Load transformed data into PostgreSQL sales_summary table and save to CSV.
    
    Args:
        **kwargs: Airflow context variables
        
    Raises:
        ValueError: If data loading fails
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
        
        # Aggregate across all dates to get total by product
        logger.info(f"Aggregating data across all dates by product_id")
        df_agg = df.groupby('product_id').agg(
            total_quantity=('total_quantity', 'sum'),
            total_sale_amount=('total_sale_amount', 'sum')
        ).reset_index()
        logger.info(f"Aggregated to {len(df_agg)} products")
        
        # Validate aggregated data
        validate_dataframe(df_agg)
        
        # Ensure proper data types
        df_agg['product_id'] = df_agg['product_id'].astype(int)
        df_agg['total_quantity'] = df_agg['total_quantity'].astype(int)
        df_agg['total_sale_amount'] = df_agg['total_sale_amount'].astype(float)
        logger.info("Data types converted successfully")
        
        # Save to CSV in data/output directory
        # This directory is mounted in the Airflow container
        output_dir = '/opt/airflow/data/output'
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'sales_summary.csv')
        
        # Check if file exists already
        if os.path.exists(output_file):
            logger.info(f"Existing sales summary file found, loading for merge: {output_file}")
            try:
                # Load existing data
                existing_df = pd.read_csv(output_file)
                
                # Ensure existing data has the required columns
                for col in ['product_id', 'total_quantity', 'total_sale_amount']:
                    if col not in existing_df.columns:
                        logger.warning(f"Existing file missing column '{col}', will overwrite file")
                        raise ValueError(f"Missing column '{col}' in existing file")
                
                # Convert columns to correct data types to ensure proper merging
                existing_df['product_id'] = existing_df['product_id'].astype(int)
                existing_df['total_quantity'] = existing_df['total_quantity'].astype(int)
                existing_df['total_sale_amount'] = existing_df['total_sale_amount'].astype(float)
                
                # Merge with new data - add quantities and amounts where product_id matches
                logger.info(f"Merging new data with existing data")
                df_merged = pd.concat([existing_df, df_agg]).groupby('product_id').agg({
                    'total_quantity': 'sum',
                    'total_sale_amount': 'sum'
                }).reset_index()
                
                # Use the merged dataframe for both saving and database loading
                df_agg = df_merged
                logger.info(f"Merged data has {len(df_agg)} products")
            except Exception as e:
                logger.warning(f"Error reading existing file, will overwrite: {str(e)}")
        
        # Ensure final data types before saving
        df_agg['product_id'] = df_agg['product_id'].astype(int)
        df_agg['total_quantity'] = df_agg['total_quantity'].astype(int)
        df_agg['total_sale_amount'] = df_agg['total_sale_amount'].astype(float)
        
        # Save the exact same data that will be loaded into PostgreSQL
        df_agg.to_csv(output_file, index=False)
        logger.info(f"Data saved to CSV file: {output_file}")
        
        # Setup database connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            # Create table if it doesn't exist (don't drop it)
            cursor.execute(CREATE_TABLE_SQL)
            logger.info("Created sales_summary table if it didn't exist")
            
            # Instead of deleting and reinserting, we'll use ON CONFLICT in the INSERT
            # to handle updating existing records by adding quantities and amounts
            logger.info("Inserting/updating records with UPSERT pattern")
            
            # Use executemany for more efficient batch insert
            records = [
                (
                    int(row['product_id']), 
                    int(row['total_quantity']), 
                    float(row['total_sale_amount'])
                ) 
                for _, row in df_agg.iterrows()
            ]
            
            if records:
                cursor.executemany(INSERT_SQL, records)
                conn.commit()
                logger.info(f"Successfully inserted/updated {len(records)} rows in database")
            else:
                logger.warning("No records to insert/update in database")
            
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