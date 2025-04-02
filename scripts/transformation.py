"""
Transformation module for ETL pipeline.
This module handles data cleaning and aggregation.
"""

import pandas as pd

def transform_data(**kwargs):
    """
    Transform and aggregate sales data from online and in-store sources.
    
    Args:
        **kwargs: Airflow context variables
        
    Returns:
        str: JSON string of transformed data
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract')
    
    # Load DataFrames from XCom
    online_df = pd.read_json(data['online_data'])
    in_store_df = pd.read_json(data['in_store_data'])
    
    # 1. Combine Data
    combined_df = pd.concat([online_df, in_store_df])
    
    # 2. Clean Data
    combined_df = combined_df.dropna()
    combined_df = combined_df[
        (combined_df['quantity'] > 0) & 
        (combined_df['sale_amount'] > 0)
    ]
    
    # 3. Aggregate
    aggregated_df = combined_df.groupby('product_id').agg(
        total_quantity=('quantity', 'sum'),
        total_sale_amount=('sale_amount', 'sum')
    ).reset_index()
    
    # Add execution date for partitioning
    aggregated_df['sale_date'] = kwargs['ds']
    
    return aggregated_df.to_json() 