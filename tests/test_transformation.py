"""
Unit tests for the transformation module.
"""

from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from scripts.transformation import (
    load_and_validate_data,
    convert_numeric_columns,
    clean_data,
    aggregate_sales,
    transform_data
)

@pytest.fixture
def test_date():
    """Provide a consistent test date."""
    return datetime.now().strftime('%Y-%m-%d')

@pytest.fixture
def sample_data(test_date):
    """Create sample input data for testing."""
    return {
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

@pytest.fixture
def sample_df(test_date):
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'product_id': ['1', '2', '3'],
        'quantity': ['2', '3', '1'],
        'sale_amount': ['20.0', '30.0', '15.0'],
        'sale_date': [test_date] * 3
    })

def test_load_and_validate_data(sample_data):
    """Test loading and validating data from XCom format."""
    online_df, in_store_df = load_and_validate_data(sample_data)
    
    assert isinstance(online_df, pd.DataFrame)
    assert isinstance(in_store_df, pd.DataFrame)
    assert not online_df.empty
    assert not in_store_df.empty

def test_load_and_validate_data_empty():
    """Test loading with no data."""
    with pytest.raises(ValueError, match="No data received from extract task"):
        load_and_validate_data(None)

def test_convert_numeric_columns(sample_df):
    """Test conversion of string columns to numeric."""
    columns = ['product_id', 'quantity', 'sale_amount']
    result = convert_numeric_columns(sample_df.copy(), columns)
    
    for col in columns:
        assert pd.api.types.is_numeric_dtype(result[col])

def test_clean_data(sample_df):
    """Test data cleaning functionality."""
    # Add some invalid data
    dirty_df = pd.concat([
        sample_df,
        pd.DataFrame({
            'product_id': ['4', '5'],
            'quantity': ['0', '-1'],
            'sale_amount': ['-10.0', '0'],
            'sale_date': [sample_df['sale_date'].iloc[0]] * 2
        })
    ])
    
    result = clean_data(dirty_df)
    
    assert len(result) < len(dirty_df)
    assert all(result['quantity'] > 0)
    assert all(result['sale_amount'] > 0)

def test_aggregate_sales(sample_df):
    """Test sales aggregation by product."""
    # Convert to numeric first
    numeric_df = convert_numeric_columns(
        sample_df,
        ['product_id', 'quantity', 'sale_amount']
    )
    
    result = aggregate_sales(numeric_df)
    
    assert 'total_quantity' in result.columns
    assert 'total_sale_amount' in result.columns
    assert len(result) <= len(sample_df)  # Should be aggregated

def test_transform_data(sample_data, test_date):
    """Test the complete transformation process."""
    # Mock task instance
    ti_mock = MagicMock()
    ti_mock.xcom_pull.return_value = sample_data
    
    # Run transformation
    result_json = transform_data(ti=ti_mock, ds=test_date)
    result_df = pd.read_json(result_json)
    
    # Verify results
    assert isinstance(result_json, str)
    assert 'product_id' in result_df.columns
    assert 'total_quantity' in result_df.columns
    assert 'total_sale_amount' in result_df.columns
    assert 'sale_date' in result_df.columns  # Transformation still includes sale_date
    # Loading will do the final aggregation by product_id only 