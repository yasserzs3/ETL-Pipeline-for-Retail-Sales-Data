"""
Unit tests for the loading module.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scripts.loading import validate_dataframe, prepare_database, load_data

@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    test_date = datetime.now().strftime('%Y-%m-%d')
    return pd.DataFrame({
        'product_id': [1, 2],
        'total_quantity': [3, 4],
        'total_sale_amount': [30.0, 40.0],
        'sale_date': [test_date, test_date]
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

def test_prepare_database():
    """Test database preparation."""
    cursor_mock = MagicMock()
    prepare_database(cursor_mock)
    cursor_mock.execute.assert_called_once()

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
    
    with patch('scripts.loading.PostgresHook') as mock_hook_class:
        mock_hook_class.return_value = pg_hook_mock
        
        # Execute load_data
        load_data(ti=ti_mock)
        
        # Verify database operations
        cursor_mock.execute.assert_called()  # CREATE TABLE
        cursor_mock.executemany.assert_called_once()  # INSERT
        conn_mock.commit.assert_called_once()
        cursor_mock.close.assert_called_once()
        conn_mock.close.assert_called_once()

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
    cursor_mock.executemany.side_effect = Exception("Database error")
    
    # Create mock task instance
    ti_mock = MagicMock()
    ti_mock.xcom_pull.return_value = sample_df.to_json()
    
    with patch('scripts.loading.PostgresHook') as mock_hook_class:
        mock_hook_class.return_value = pg_hook_mock
        
        with pytest.raises(Exception):
            load_data(ti=ti_mock)
        
        # Verify rollback was called
        conn_mock.rollback.assert_called_once()
        cursor_mock.close.assert_called_once()
        conn_mock.close.assert_called_once() 