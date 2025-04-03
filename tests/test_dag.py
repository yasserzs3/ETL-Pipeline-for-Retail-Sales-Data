"""
Unit tests for the ETL pipeline DAG.
"""

from datetime import datetime
import pytest
from airflow.models import DagBag

def test_dag_loaded():
    """Test that the DAG is loaded correctly."""
    dagbag = DagBag()
    dag = dagbag.get_dag(dag_id='retail_sales_etl')
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 3

def test_dag_structure():
    """Test the structure of the DAG."""
    dagbag = DagBag()
    dag = dagbag.get_dag(dag_id='retail_sales_etl')
    
    # Get tasks
    extract_task = dag.get_task('extract')
    transform_task = dag.get_task('transform')
    load_task = dag.get_task('load')
    
    # Test task existence
    assert extract_task is not None
    assert transform_task is not None
    assert load_task is not None
    
    # Test dependencies
    upstream_task_ids = lambda task: [t.task_id for t in task.upstream_list]
    downstream_task_ids = lambda task: [t.task_id for t in task.downstream_list]
    
    assert upstream_task_ids(extract_task) == []
    assert downstream_task_ids(extract_task) == ['transform']
    
    assert upstream_task_ids(transform_task) == ['extract']
    assert downstream_task_ids(transform_task) == ['load']
    
    assert upstream_task_ids(load_task) == ['transform']
    assert downstream_task_ids(load_task) == []

def test_dag_default_args():
    """Test the default arguments of the DAG."""
    dagbag = DagBag()
    dag = dagbag.get_dag(dag_id='retail_sales_etl')
    
    assert dag.default_args['owner'] == 'airflow'
    assert dag.default_args['depends_on_past'] is False
    assert dag.default_args['email_on_failure'] is True
    assert dag.default_args['retries'] == 2
    
    # Check schedule
    assert dag.schedule_interval == '@daily'
    assert dag.catchup is False

def test_task_documentation():
    """Test that all tasks have documentation."""
    dagbag = DagBag()
    dag = dagbag.get_dag(dag_id='retail_sales_etl')
    
    for task in dag.tasks:
        assert task.doc_md is not None
        assert len(task.doc_md) > 0 