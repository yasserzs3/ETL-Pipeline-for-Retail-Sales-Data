"""
Transformation module for ETL pipeline.

This module handles data cleaning, validation, and aggregation of sales data.
"""

import logging
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_and_validate_data(data):
    """
    Load and validate data from XCom.
    
    Args:
        data: Dictionary containing JSON strings of online and in-store data
        
    Returns:
        Tuple of online_df, in_store_df
    """
    if not data:
        raise ValueError("No data received from extract task")
        
    logger.info("Loading DataFrames from XCom")
    online_df = pd.read_json(data['online_data'])
    in_store_df = pd.read_json(data['in_store_data'])
    
    logger.info(f"Loaded online data: {online_df.shape} rows")
    logger.info(f"Loaded in-store data: {in_store_df.shape} rows")
    
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
    # Remove rows with null values
    df = df.dropna()
    
    # Filter out invalid quantities and amounts
    df = df[
        (df['quantity'] > 0) & 
        (df['sale_amount'] > 0)
    ]
    
    logger.info(f"Data shape after cleaning: {df.shape}")
    return df

def aggregate_sales(df):
    """
    Aggregate sales data by product.
    
    Args:
        df: DataFrame to aggregate
        
    Returns:
        Aggregated DataFrame
    """
    return df.groupby('product_id').agg(
        total_quantity=('quantity', 'sum'),
        total_sale_amount=('sale_amount', 'sum')
    ).reset_index()

def transform_data(**kwargs):
    """
    Transform and aggregate sales data from online and in-store sources.
    
    Args:
        **kwargs: Airflow context variables
        
    Returns:
        JSON string of transformed data
    """
    try:
        # Load and validate data
        ti = kwargs['ti']
        online_df, in_store_df = load_and_validate_data(ti.xcom_pull(task_ids='extract'))
        
        # Convert string columns to numeric
        numeric_columns = ['product_id', 'quantity', 'sale_amount']
        online_df = convert_numeric_columns(online_df, numeric_columns)
        in_store_df = convert_numeric_columns(in_store_df, numeric_columns)
        
        # Combine data sources
        logger.info("Combining data sources")
        combined_df = pd.concat([online_df, in_store_df])
        logger.info(f"Combined data shape: {combined_df.shape}")
        
        # Clean and validate
        combined_df = clean_data(combined_df)
        
        # Aggregate sales
        logger.info("Aggregating sales data")
        aggregated_df = aggregate_sales(combined_df)
        
        # Add execution date
        aggregated_df['sale_date'] = kwargs['ds']
        
        logger.info(f"Final aggregated data shape: {aggregated_df.shape}")
        return aggregated_df.to_json()
        
    except Exception as e:
        logger.error(f"Transform task failed: {str(e)}")
        raise

if __name__ == "__main__":
    # For testing
    from datetime import datetime
    
    test_date = datetime.now().strftime('%Y-%m-%d')
    test_data = {
        'online_data': pd.DataFrame({
            'product_id': ['1', '2'],
            'quantity': ['2', '3'],
            'sale_amount': ['20.0', '30.0'],
            'sale_date': [test_date, test_date]
        }).to_json(),
        'in_store_data': pd.DataFrame({
            'product_id': ['1', '3'],
            'quantity': ['1', '2'],
            'sale_amount': ['10.0', '25.0'],
            'sale_date': [test_date, test_date]
        }).to_json()
    }
    
    class TestContext:
        def xcom_pull(self, task_ids):
            return test_data
    
    result = transform_data(ti=TestContext(), ds=test_date)
    print("Transform test successful") 