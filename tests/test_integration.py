"""
Integration tests for the ETL pipeline.
"""

import os
from datetime import datetime
import tempfile
from unittest.mock import patch

import pandas as pd
import pytest
from airflow.models import TaskInstance, DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scripts.extraction import extract_data
from scripts.transformation import transform_data
from scripts.loading import load_data

@pytest.fixture
def test_dag():
    """Create a test DAG."""
    return DAG(
        'test_dag',
        default_args={'owner': 'airflow'},
        schedule_interval='@daily',
        start_date=days_ago(2)
    )

@pytest.fixture
def setup_test_data(test_data_dir):
    """Set up test data in both PostgreSQL and CSV."""
    # Create test CSV
    csv_path = os.path.join(test_data_dir, 'test_sales.csv')
    test_date = datetime.now().strftime('%Y-%m-%d')
    
    csv_df = pd.DataFrame({
        'product_id': [1, 2, 3],
        'quantity': [2, 3, 1],
        'sale_amount': [20.0, 30.0, 15.0],
        'sale_date': [test_date] * 3
    })
    
    csv_df.to_csv(csv_path, index=False)
    
    # Setup PostgreSQL test data
    with patch('scripts.extraction.CSV_PATH', csv_path):
        yield csv_path

def test_full_pipeline(test_dag, setup_test_data):
    """Test the complete ETL pipeline."""
    # Create task instances
    extract_ti = TaskInstance(task=test_dag.task('extract'), execution_date=days_ago(1))
    transform_ti = TaskInstance(task=test_dag.task('transform'), execution_date=days_ago(1))
    load_ti = TaskInstance(task=test_dag.task('load'), execution_date=days_ago(1))
    
    # Run extraction
    extract_result = extract_data(ti=extract_ti)
    assert isinstance(extract_result, dict)
    assert 'online_data' in extract_result
    assert 'in_store_data' in extract_result
    
    # Run transformation
    with patch.object(transform_ti, 'xcom_pull', return_value=extract_result):
        transform_result = transform_data(ti=transform_ti, ds=days_ago(1).strftime('%Y-%m-%d'))
    assert transform_result is not None
    
    # Run loading
    with patch.object(load_ti, 'xcom_pull', return_value=transform_result):
        load_data(ti=load_ti)
    
    # Verify final results in database
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    result_df = pg_hook.get_pandas_df(
        "SELECT * FROM sales_summary ORDER BY product_id"
    )
    
    assert not result_df.empty
    assert all(col in result_df.columns 
              for col in ['product_id', 'total_quantity', 'total_sale_amount'])
    assert len(result_df) > 0

@pytest.mark.performance
def test_pipeline_performance(test_dag, setup_test_data):
    """Test pipeline performance with larger dataset."""
    # Generate larger test dataset
    test_date = datetime.now().strftime('%Y-%m-%d')
    large_df = pd.DataFrame({
        'product_id': range(1000),
        'quantity': [i % 10 + 1 for i in range(1000)],
        'sale_amount': [i * 10.0 for i in range(1000)],
        'sale_date': [test_date] * 1000
    })
    
    # Create task instances
    extract_ti = TaskInstance(task=test_dag.task('extract'), execution_date=days_ago(1))
    transform_ti = TaskInstance(task=test_dag.task('transform'), execution_date=days_ago(1))
    load_ti = TaskInstance(task=test_dag.task('load'), execution_date=days_ago(1))
    
    # Mock extraction result
    extract_result = {
        'online_data': large_df[:500].to_json(),
        'in_store_data': large_df[500:].to_json()
    }
    
    # Test transformation performance
    with patch.object(transform_ti, 'xcom_pull', return_value=extract_result):
        transform_result = transform_data(ti=transform_ti, ds=test_date)
    
    # Test loading performance
    with patch.object(load_ti, 'xcom_pull', return_value=transform_result):
        load_data(ti=load_ti)
    
    # Verify results
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    result_df = pg_hook.get_pandas_df(
        "SELECT COUNT(*) as count FROM sales_summary"
    )
    assert result_df['count'].iloc[0] == 1000 