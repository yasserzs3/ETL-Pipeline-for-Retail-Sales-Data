# ETL Pipeline for Retail Sales Data

## DAG Design Overview

The ETL pipeline is designed to process daily retail sales data from multiple sources using Apache Airflow. The DAG follows a linear workflow with three main tasks: Extract, Transform, and Load.

```
Extract → Transform → Load
```

### Task Details

#### 1. Extract Task
- **Operator**: `PythonOperator`
- **Function**: `extract_data`
- **Purpose**: Fetches sales data from multiple sources
- **Implementation**:
  - Extracts online sales from PostgreSQL database
  - Reads in-store sales from CSV files
  - Validates data structure and required columns
  - Returns data via XCom for next task
- **Error Handling**:
  - Validates data source availability
  - Checks for required columns
  - Ensures data format consistency

#### 2. Transform Task
- **Operator**: `PythonOperator`
- **Function**: `transform_data`
- **Purpose**: Processes and aggregates sales data
- **Implementation**:
  - Combines online and in-store sales data
  - Cleans invalid or null entries
  - Aggregates sales by product_id
  - Calculates daily totals
- **Data Validation**:
  - Checks data types
  - Handles missing values
  - Validates calculations

#### 3. Load Task
- **Operator**: `PythonOperator`
- **Function**: `load_data`
- **Purpose**: Loads processed data into target database
- **Implementation**:
  - Creates/updates sales_summary table
  - Implements upsert logic
  - Ensures data integrity
- **Error Handling**:
  - Transaction management
  - Connection error handling
  - Duplicate record handling

### Operator Choice Rationale

The `PythonOperator` was chosen for all tasks for the following reasons:

1. **Flexibility**:
   - Allows complex data processing logic
   - Supports custom error handling
   - Enables detailed logging

2. **Data Manipulation**:
   - Easy integration with pandas for data processing
   - Efficient handling of different data formats
   - Custom validation logic implementation

3. **Code Organization**:
   - Modular code structure
   - Reusable functions
   - Clear separation of concerns

4. **Error Management**:
   - Custom exception handling
   - Detailed error reporting
   - Retry logic control

## Implementation Details

### Data Flow
```
PostgreSQL (online_sales)  →
                            Extract  →  Transform  →  Load  →  PostgreSQL (sales_summary)
CSV (in_store_sales)      →
```

### Key Features

1. **Data Validation**:
   - Schema validation
   - Data type checking
   - Null value handling

2. **Error Handling**:
   - Comprehensive logging
   - Graceful failure handling
   - Retry mechanisms

3. **Performance Optimization**:
   - Batch processing
   - Efficient SQL queries
   - Memory management

### Configuration

```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 1),
}

dag = DAG(
    'retail_sales_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)
```

### Dependencies
- Apache Airflow
- PostgreSQL
- Python 3.x
- pandas
- psycopg2-binary

## Code Documentation

Each task in the pipeline is thoroughly documented with docstrings and comments explaining:
- Purpose and functionality
- Input/output specifications
- Error handling mechanisms
- Data validation steps
- Performance considerations

Example from the transform task:
```python
def transform_data(**context):
    """
    Transforms and aggregates sales data from multiple sources.
    
    Steps:
    1. Retrieves data from XCom
    2. Combines online and in-store sales
    3. Cleans and validates data
    4. Aggregates by product_id
    
    Returns:
        str: JSON string of transformed data
    
    Raises:
        ValueError: If data validation fails
        TypeError: If data type conversion fails
    """
``` 