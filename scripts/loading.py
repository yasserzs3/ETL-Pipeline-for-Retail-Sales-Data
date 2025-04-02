"""
Loading module for ETL pipeline.
This module handles loading data into PostgreSQL.
"""

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_data(**kwargs):
    """
    Load transformed data into PostgreSQL sales_summary table.
    
    Args:
        **kwargs: Airflow context variables
    """
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='transform')
    df = pd.read_json(data_json)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Batch insert
    tuples = list(df.itertuples(index=False, name=None))
    query = """
        INSERT INTO sales_summary 
        (product_id, total_quantity, total_sale_amount, sale_date)
        VALUES (%s, %s, %s, %s)
    """
    cursor.executemany(query, tuples)
    conn.commit() 