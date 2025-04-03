"""
Unit tests for the loading module.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scripts.loading import validate_dataframe, load_data

@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'product_id': [1, 2],
        'total_quantity': [3, 4],
        'total_sale_amount': [30.0, 40.0]
    })

@pytest.fixture
def pg_hook_mock():
    """Create a mock PostgreSQL hook."""
    mock = MagicMock(spec=PostgresHook)
    return mock

def test_validate_dataframe_valid(sample_df):
    """Test DataFrame validation with valid data."""
    validate_dataframe(sample_df)  # Should not raise any exception

def test_validate_dataframe_empty():
    """Test DataFrame validation with empty DataFrame."""
    empty_df = pd.DataFrame()
    with pytest.raises(ValueError, match="DataFrame is empty"):
        validate_dataframe(empty_df)

def test_validate_dataframe_missing_columns(sample_df):
    """Test DataFrame validation with missing columns."""
    df_missing = sample_df.drop('total_quantity', axis=1)
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_dataframe(df_missing)

def test_validate_dataframe_negative_quantity(sample_df):
    """Test DataFrame validation with negative quantities."""
    sample_df.loc[0, 'total_quantity'] = -1
    with pytest.raises(ValueError, match="Found negative quantities"):
        validate_dataframe(sample_df)

def test_validate_dataframe_negative_amount(sample_df):
    """Test DataFrame validation with negative amounts."""
    sample_df.loc[0, 'total_sale_amount'] = -1
    with pytest.raises(ValueError, match="Found negative sale amounts"):
        validate_dataframe(sample_df)

def test_load_data_success(sample_df, pg_hook_mock):
    """Test successful data loading."""
    # Mock the database connection and cursor
    conn_mock = MagicMock()
    cursor_mock = MagicMock()
    pg_hook_mock.get_conn.return_value = conn_mock
    conn_mock.cursor.return_value = cursor_mock
    
    # Create mock task instance
    ti_mock = MagicMock()
    ti_mock.xcom_pull.return_value = sample_df.to_json()
    
    # Mock os.makedirs and dataframe to_csv
    with patch('scripts.loading.PostgresHook') as mock_hook_class, \
         patch('scripts.loading.os.makedirs') as mock_makedirs, \
         patch('scripts.loading.os.path.exists') as mock_exists, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv:
        
        mock_hook_class.return_value = pg_hook_mock
        mock_exists.return_value = False  # Simulate no existing CSV file
        
        # Execute load_data with required ds parameter
        load_data(ti=ti_mock, ds="2025-04-01")
        
        # Verify database operations
        # Should execute CREATE TABLE IF NOT EXISTS and then executemany for batch inserts
        assert cursor_mock.execute.call_count >= 1  # At least CREATE TABLE
        assert cursor_mock.executemany.call_count == 1  # Batch insert
        conn_mock.commit.assert_called_once()
        cursor_mock.close.assert_called_once()
        conn_mock.close.assert_called_once()
        mock_makedirs.assert_called_once()
        mock_to_csv.assert_called_once()

def test_load_data_no_data():
    """Test load_data with no input data."""
    ti_mock = MagicMock()
    ti_mock.xcom_pull.return_value = None
    
    with pytest.raises(ValueError, match="No data received from transform task"):
        load_data(ti=ti_mock)

def test_load_data_database_error(sample_df, pg_hook_mock):
    """Test load_data with database error."""
    # Mock database error
    conn_mock = MagicMock()
    cursor_mock = MagicMock()
    pg_hook_mock.get_conn.return_value = conn_mock
    conn_mock.cursor.return_value = cursor_mock
    # Make the execute call fail with an exception
    cursor_mock.execute.side_effect = Exception("Database error")
    
    # Create mock task instance
    ti_mock = MagicMock()
    ti_mock.xcom_pull.return_value = sample_df.to_json()
    
    with patch('scripts.loading.PostgresHook') as mock_hook_class, \
         patch('scripts.loading.os.makedirs') as _, \
         patch('scripts.loading.os.path.exists') as mock_exists:
        
        mock_hook_class.return_value = pg_hook_mock
        mock_exists.return_value = False  # Simulate no existing CSV file
        
        with pytest.raises(Exception):
            load_data(ti=ti_mock, ds="2025-04-01")
        
        # Verify rollback was called
        conn_mock.rollback.assert_called_once()
        cursor_mock.close.assert_called_once()
        conn_mock.close.assert_called_once() 