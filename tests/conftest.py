"""
Pytest configuration file with shared fixtures.
"""

import os
import tempfile
from datetime import datetime

import pandas as pd
import pytest

@pytest.fixture(scope="session")
def test_data_dir():
    """Create a temporary directory for test data files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

@pytest.fixture(scope="session")
def sample_csv_file(test_data_dir):
    """Create a sample CSV file for testing."""
    csv_path = os.path.join(test_data_dir, "test_sales.csv")
    test_date = datetime.now().strftime('%Y-%m-%d')
    
    df = pd.DataFrame({
        'product_id': [1, 2, 3],
        'quantity': [2, 3, 1],
        'sale_amount': [20.0, 30.0, 15.0],
        'sale_date': [test_date] * 3
    })
    
    df.to_csv(csv_path, index=False)
    return csv_path

@pytest.fixture(scope="session")
def test_execution_date():
    """Provide a consistent test execution date."""
    return datetime.now().strftime('%Y-%m-%d') 