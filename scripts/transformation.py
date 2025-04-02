"""
Transformation module for ETL pipeline.
This module handles data cleaning, merging, and aggregation.
"""

import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_data(df, source_name):
    """
    Clean the input DataFrame by handling null values, 
    standardizing date formats, and removing incorrect entries.
    
    Args:
        df: DataFrame to clean
        source_name: Name of data source for logging
        
    Returns:
        pandas.DataFrame: Cleaned data
    """
    try:
        original_shape = df.shape
        logger.info(f"Starting data cleaning for {source_name} data: {original_shape}")
        
        # Drop rows with null values in critical columns
        critical_columns = ['product_id', 'date', 'quantity', 'price']
        df = df.dropna(subset=critical_columns)
        logger.info(f"After dropping nulls in critical columns: {df.shape}")
        
        # Convert product_id to integer if needed
        if df['product_id'].dtype != 'int64':
            df['product_id'] = df['product_id'].astype(int)
        
        # Standardize date format
        if df['date'].dtype == 'object':
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            # Drop rows where date conversion failed
            df = df.dropna(subset=['date'])
            logger.info(f"After standardizing dates: {df.shape}")
            
        # Ensure numeric columns are numeric
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
        
        # Remove rows with invalid numeric values
        df = df.dropna(subset=['quantity', 'price', 'total_amount'])
        logger.info(f"After ensuring numeric values: {df.shape}")
        
        # Filter out invalid values (negative prices, quantities)
        df = df[(df['quantity'] > 0) & (df['price'] > 0)]
        logger.info(f"After removing invalid values: {df.shape}")
        
        # Recalculate total_amount for consistency
        df['total_amount'] = df['quantity'] * df['price']
        
        # Log cleaning summary
        removed_rows = original_shape[0] - df.shape[0]
        removed_percentage = (removed_rows / original_shape[0]) * 100 if original_shape[0] > 0 else 0
        logger.info(f"Cleaning removed {removed_rows} rows ({removed_percentage:.2f}%) from {source_name} data")
        
        return df
    except Exception as e:
        logger.error(f"Error cleaning {source_name} data: {str(e)}")
        raise

def merge_data(df_online, df_instore):
    """
    Merge online and in-store sales data.
    
    Args:
        df_online: Online sales DataFrame
        df_instore: In-store sales DataFrame
        
    Returns:
        pandas.DataFrame: Merged sales data
    """
    try:
        # Add source column to identify data origin
        df_online['source'] = 'online'
        df_instore['source'] = 'in_store'
        
        # Ensure both DataFrames have the same columns
        online_cols = set(df_online.columns)
        instore_cols = set(df_instore.columns)
        
        # Add store_id column to online data if it doesn't exist
        if 'store_id' in instore_cols and 'store_id' not in online_cols:
            df_online['store_id'] = 'ONLINE'
            
        # Combine the DataFrames
        df_combined = pd.concat([df_online, df_instore], ignore_index=True)
        
        logger.info(f"Successfully merged data: {len(df_online)} online records + {len(df_instore)} in-store records = {len(df_combined)} total records")
        return df_combined
    except Exception as e:
        logger.error(f"Error merging data: {str(e)}")
        raise

def aggregate_data(df):
    """
    Aggregate data for analysis.
    
    Args:
        df: DataFrame to aggregate
        
    Returns:
        pandas.DataFrame: Aggregated sales data
    """
    try:
        # Aggregate by product_id
        product_agg = df.groupby('product_id').agg(
            total_quantity=('quantity', 'sum'),
            total_revenue=('total_amount', 'sum'),
            avg_price=('price', 'mean'),
            transaction_count=('quantity', 'count')
        ).reset_index()
        
        # Aggregate by product_id and source
        product_source_agg = df.groupby(['product_id', 'source']).agg(
            total_quantity=('quantity', 'sum'),
            total_revenue=('total_amount', 'sum')
        ).reset_index()
        
        # Aggregate by date
        date_agg = df.groupby('date').agg(
            total_quantity=('quantity', 'sum'),
            total_revenue=('total_amount', 'sum'),
            product_count=('product_id', 'nunique')
        ).reset_index()
        
        # Aggregate by product_id and date
        product_date_agg = df.groupby(['product_id', 'date']).agg(
            total_quantity=('quantity', 'sum'),
            total_revenue=('total_amount', 'sum')
        ).reset_index()
        
        # If store_id exists, aggregate by store
        if 'store_id' in df.columns:
            store_agg = df.groupby('store_id').agg(
                total_quantity=('quantity', 'sum'),
                total_revenue=('total_amount', 'sum'),
                product_count=('product_id', 'nunique')
            ).reset_index()
        else:
            store_agg = None
            
        logger.info("Aggregation complete")
        
        return {
            'product_agg': product_agg,
            'product_source_agg': product_source_agg,
            'date_agg': date_agg,
            'product_date_agg': product_date_agg,
            'store_agg': store_agg
        }
    except Exception as e:
        logger.error(f"Error aggregating data: {str(e)}")
        raise

def validate_transformed_data(df, aggregations):
    """
    Validate the transformed data for consistency.
    
    Args:
        df: Original merged DataFrame
        aggregations: Dictionary of aggregated DataFrames
        
    Returns:
        bool: True if validation passes
    """
    try:
        # Check if sum of quantities match between original and aggregated
        original_quantity = df['quantity'].sum()
        agg_quantity = aggregations['product_agg']['total_quantity'].sum()
        
        if not np.isclose(original_quantity, agg_quantity):
            logger.error(f"Quantity sum mismatch: Original={original_quantity}, Aggregated={agg_quantity}")
            return False
            
        # Check if sum of revenue matches between original and aggregated
        original_revenue = df['total_amount'].sum()
        agg_revenue = aggregations['product_agg']['total_revenue'].sum()
        
        if not np.isclose(original_revenue, agg_revenue):
            logger.error(f"Revenue sum mismatch: Original={original_revenue}, Aggregated={agg_revenue}")
            return False
            
        logger.info("Data validation successful")
        return True
    except Exception as e:
        logger.error(f"Error validating transformed data: {str(e)}")
        return False

def save_transformed_data(df, aggregations, output_dir=None):
    """
    Save the transformed and aggregated data.
    
    Args:
        df: Merged DataFrame
        aggregations: Dictionary of aggregated DataFrames
        output_dir: Directory to save files (default: data/output)
        
    Returns:
        dict: Paths to saved files
    """
    try:
        if output_dir is None:
            output_dir = os.path.join('data', 'output')
            
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Create timestamp for filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save merged data
        merged_path = os.path.join(output_dir, f"merged_sales_{timestamp}.csv")
        df.to_csv(merged_path, index=False)
        
        # Save aggregations
        agg_paths = {}
        for agg_name, agg_df in aggregations.items():
            if agg_df is not None:
                path = os.path.join(output_dir, f"{agg_name}_{timestamp}.csv")
                agg_df.to_csv(path, index=False)
                agg_paths[agg_name] = path
                
        logger.info(f"All transformed data saved to {output_dir}")
        return {"merged": merged_path, "aggregations": agg_paths}
    except Exception as e:
        logger.error(f"Error saving transformed data: {str(e)}")
        raise

def transform(df_online, df_instore):
    """
    Main transformation function.
    
    Args:
        df_online: Online sales DataFrame
        df_instore: In-store sales DataFrame
        
    Returns:
        tuple: (Merged DataFrame, Dictionary of aggregations, Dictionary of file paths)
    """
    try:
        # Clean data
        df_online_clean = clean_data(df_online, "online")
        df_instore_clean = clean_data(df_instore, "in-store")
        
        # Merge data
        df_merged = merge_data(df_online_clean, df_instore_clean)
        
        # Aggregate data
        aggregations = aggregate_data(df_merged)
        
        # Validate transformed data
        if not validate_transformed_data(df_merged, aggregations):
            logger.warning("Data validation failed, but continuing with transformation")
            
        # Save transformed data
        saved_paths = save_transformed_data(df_merged, aggregations)
        
        return df_merged, aggregations, saved_paths
    except Exception as e:
        logger.error(f"Transformation process failed: {str(e)}")
        raise

if __name__ == "__main__":
    # For testing
    try:
        # Import extraction to get data
        from extraction import extract
        
        # Extract data
        df_online, df_instore = extract()
        
        # Transform data
        merged, aggregations, paths = transform(df_online, df_instore)
        
        print(f"Merged data shape: {merged.shape}")
        print(f"Product aggregation shape: {aggregations['product_agg'].shape}")
        print(f"Files saved to: {paths}")
    except Exception as e:
        print(f"Error: {str(e)}") 