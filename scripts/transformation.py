"""
Transformation module for ETL pipeline.

This module handles data cleaning, validation, and aggregation of sales data.
"""

import pandas as pd
import logging

def load_and_validate_data(data):
    """
    Load and validate data from XCom.
    
    Args:
        data: Dictionary containing JSON strings of online and in-store data
        
    Returns:
        Tuple of online_df, in_store_df
    """
    logger = logging.getLogger(__name__)
    
    if not data:
        logger.error("No data received from extract task")
        raise ValueError("No data received from extract task")
        
    online_df = pd.read_json(data['online_data'])
    in_store_df = pd.read_json(data['in_store_data'])
    
    logger.info(f"Loaded online data: {len(online_df)} rows")
    logger.info(f"Loaded in-store data: {len(in_store_df)} rows")
    
    return online_df, in_store_df

def convert_numeric_columns(df, columns):
    """
    Convert specified columns to numeric type.
    
    Args:
        df: DataFrame to process
        columns: List of columns to convert
        
    Returns:
        DataFrame with converted columns
    """
    for col in columns:
        df[col] = pd.to_numeric(df[col])
    return df

def clean_data(df):
    """
    Clean data by removing invalid entries.
    
    Args:
        df: DataFrame to clean
        
    Returns:
        Cleaned DataFrame
    """
    logger = logging.getLogger(__name__)
    
    # Remove rows with null values
    df_cleaned = df.dropna()
    if len(df_cleaned) < len(df):
        logger.info(f"Removed {len(df) - len(df_cleaned)} rows with null values")
    
    # Filter out invalid quantities and amounts
    df_final = df_cleaned[
        (df_cleaned['quantity'] > 0) & 
        (df_cleaned['sale_amount'] > 0)
    ]
    
    if len(df_final) < len(df_cleaned):
        logger.info(f"Removed {len(df_cleaned) - len(df_final)} rows with invalid quantities or amounts")
    
    return df_final

def aggregate_sales(df):
    """
    Aggregate sales data by product and date.
    
    Args:
        df: DataFrame to aggregate
        
    Returns:
        Aggregated DataFrame
    """
    logger = logging.getLogger(__name__)
    
    # Aggregate by product_id and sale_date
    aggregated = df.groupby(['product_id', 'sale_date']).agg(
        total_quantity=('quantity', 'sum'),
        total_sale_amount=('sale_amount', 'sum')
    ).reset_index()
    
    logger.info(f"Aggregated data from {len(df)} rows to {len(aggregated)} rows")
    
    return aggregated

def transform_data(**kwargs):
    """
    Transform and aggregate sales data from online and in-store sources.
    
    Args:
        **kwargs: Airflow context variables
        
    Returns:
        JSON string of transformed data
    """
    try:
        # Set up logger
        logger = logging.getLogger(__name__)
        logger.info("Starting data transformation")
        
        # Load and validate data
        ti = kwargs['ti']
        online_df, in_store_df = load_and_validate_data(ti.xcom_pull(task_ids='extract'))
        
        # Convert string columns to numeric
        numeric_columns = ['sale_id', 'product_id', 'quantity', 'sale_amount']
        online_df = convert_numeric_columns(online_df, numeric_columns)
        in_store_df = convert_numeric_columns(in_store_df, numeric_columns)
        logger.info("Converted numeric columns")
        
        # Ensure date column is in datetime format
        online_df['sale_date'] = pd.to_datetime(online_df['sale_date']).dt.date
        in_store_df['sale_date'] = pd.to_datetime(in_store_df['sale_date']).dt.date
        logger.info("Converted date columns")
        
        # Combine data sources
        combined_df = pd.concat([online_df, in_store_df])
        logger.info(f"Combined data: {len(combined_df)} rows")
        
        # Clean and validate
        combined_df = clean_data(combined_df)
        logger.info(f"After cleaning: {len(combined_df)} rows")
        
        # Aggregate sales - keep the original sale_date from the data
        aggregated_df = aggregate_sales(combined_df)
        logger.info(f"After aggregation: {len(aggregated_df)} rows")
        
        logger.info("Data transformation complete")
        return aggregated_df.to_json(date_format='iso')
        
    except Exception as e:
        logger.error(f"Transform error: {str(e)}")
        raise 