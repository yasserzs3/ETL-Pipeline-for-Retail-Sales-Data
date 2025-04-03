"""
Unit tests for the extraction module.
"""

import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scripts.extraction import (
    validate_dataframe,
    extract_online_sales,
    extract_store_sales,
    extract_data,
    REQUIRED_COLUMNS
)

@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'product_id': ['1', '2'],
        'quantity': ['2', '3'],
        'sale_amount': ['20.0', '30.0'],
        'sale_date': ['2025-03-01', '2025-03-01']
    })

@pytest.fixture
def pg_hook_mock():
    """Create a mock PostgreSQL hook."""
    mock = MagicMock(spec=PostgresHook)
    return mock

def test_validate_dataframe_valid(sample_df):
    """Test DataFrame validation with valid data."""
    assert validate_dataframe(sample_df, REQUIRED_COLUMNS) is True

def test_validate_dataframe_empty():
    """Test DataFrame validation with empty DataFrame."""
    empty_df = pd.DataFrame()
    with pytest.raises(ValueError, match="DataFrame is empty"):
        validate_dataframe(empty_df, REQUIRED_COLUMNS)

def test_validate_dataframe_missing_columns(sample_df):
    """Test DataFrame validation with missing columns."""
    df_missing = sample_df.drop('quantity', axis=1)
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_dataframe(df_missing, REQUIRED_COLUMNS)

def test_extract_online_sales(pg_hook_mock, sample_df):
    """Test extraction of online sales data."""
    pg_hook_mock.get_pandas_df.return_value = sample_df
    result = extract_online_sales(pg_hook_mock)
    
    assert isinstance(result, pd.DataFrame)
    assert all(col in result.columns for col in REQUIRED_COLUMNS)
    pg_hook_mock.get_pandas_df.assert_called_once()

@patch('scripts.extraction.os.path.exists')
@patch('scripts.extraction.pd.read_csv')
def test_extract_store_sales(mock_read_csv, mock_exists, sample_df):
    """Test extraction of in-store sales data."""
    mock_exists.return_value = True
    mock_read_csv.return_value = sample_df
    
    result = extract_store_sales()
    
    assert isinstance(result, pd.DataFrame)
    assert all(col in result.columns for col in REQUIRED_COLUMNS)
    mock_read_csv.assert_called_once()

@patch('scripts.extraction.os.path.exists')
def test_extract_store_sales_missing_file(mock_exists):
    """Test extraction when CSV file is missing."""
    mock_exists.return_value = False
    
    with pytest.raises(FileNotFoundError):
        extract_store_sales()

def test_extract_data(pg_hook_mock, sample_df):
    """Test the main extract_data function."""
    with patch('scripts.extraction.PostgresHook') as mock_hook_class, \
         patch('scripts.extraction.extract_store_sales') as mock_store:
        
        mock_hook_class.return_value = pg_hook_mock
        pg_hook_mock.get_pandas_df.return_value = sample_df
        mock_store.return_value = sample_df
        
        test_date = datetime.now().strftime('%Y-%m-%d')
        result = extract_data(ds=test_date)
        
        assert isinstance(result, dict)
        assert 'online_data' in result
        assert 'in_store_data' in result
        
        # Verify both DataFrames can be reconstructed from JSON
        online_df = pd.read_json(result['online_data'])
        in_store_df = pd.read_json(result['in_store_data'])
        
        assert all(col in online_df.columns for col in REQUIRED_COLUMNS)
        assert all(col in in_store_df.columns for col in REQUIRED_COLUMNS) 