# ETL Pipeline for Retail Sales Data

## Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline for processing daily retail sales data from multiple sources using Apache Airflow. The pipeline combines online sales data from a PostgreSQL database with in-store sales from CSV files, processes the combined data, and loads the results into a centralized sales summary database.

## Project Structure

```
MLOps-HW1/
├── dags/                 # Airflow DAG definitions
│   └── etl_pipeline.py   # Main ETL DAG definition
├── scripts/              # Implementation of ETL tasks
│   ├── extraction.py     # Data extraction functions
│   ├── transformation.py # Data transformation logic
│   └── loading.py        # Database loading functions
├── data/                 # Data directories
│   ├── input/            # Source data files
│   └── output/           # Output data (if saved locally)
├── tests/                # Unit and integration tests
├── docker/               # Docker configuration
│   ├── Dockerfile        # Airflow service definition
│   └── docker-compose.yml# Multi-container setup
└── requirements.txt      # Python dependencies
```

## DAG Workflow

The ETL pipeline follows a linear workflow with three main tasks:

```
Extract → Transform → Load
```

### Task Details

#### 1. Extract Task
- **Implementation**: `scripts/extraction.py`
- **Purpose**: Fetches sales data from multiple sources
- **Data Sources**:
  - Online sales from PostgreSQL database
  - In-store sales from CSV files in the data/input directory
- **Features**:
  - Data validation and schema checking
  - Error handling for connection issues
  - Returns structured data for the transformation step
- **Key Functions**:
  - `extract_data()`: Main entry point that orchestrates extraction from all sources and returns JSON strings via XCom
  - `extract_online_sales()`: Retrieves sales data from PostgreSQL database, converting columns to string format
  - `extract_store_sales()`: Reads CSV files and formats data (date formatting, converting numeric columns to strings)
  - `setup_source_data()`: Creates tables and populates with sample data if they don't exist
  - `validate_dataframe()`: Checks if the DataFrame is non-empty and contains all required columns

#### 2. Transform Task
- **Implementation**: `scripts/transformation.py`
- **Purpose**: Processes and aggregates sales data
- **Key Operations**:
  - Combines online and in-store sales data
  - Cleans invalid or null entries
  - Aggregates sales by product_id and sale_date
  - Calculates total quantities and amounts
- **Data Validation**:
  - Type checking and conversion
  - Handling of missing values
  - Data quality assurance
- **Key Functions**:
  - `transform_data()`: Main function that orchestrates the transformation process and returns JSON data
  - `load_and_validate_data()`: Loads JSON strings from XCom and converts to DataFrames
  - `convert_numeric_columns()`: Converts string columns to numeric types using pandas to_numeric
  - `clean_data()`: Removes rows with null values and filters out records with non-positive quantities or amounts
  - `aggregate_sales()`: Groups by product_id and sale_date to sum quantities and sale amounts

#### 3. Load Task
- **Implementation**: `scripts/loading.py`
- **Purpose**: Loads processed data into target database
- **Features**:
  - Creates or updates sales_summary table
  - Implements upsert pattern for updating existing records
  - Maintains both database and CSV output
  - Transaction management for data integrity
- **Key Functions**:
  - `load_data()`: Main function that retrieves data from XCom, processes it, and loads to both CSV and database
  - `validate_dataframe()`: Checks for required columns and ensures quantities and amounts aren't negative
  - Uses SQL with ON CONFLICT to update existing records by adding quantities and amounts
  - Merges new data with existing CSV data if output file already exists
  - Saves final aggregated product-level summary (not date-level) to both database and CSV

## Implementation Details

### Technologies Used

- **Apache Airflow**: Workflow orchestration
- **PostgreSQL**: Database storage
- **Pandas**: Data manipulation and processing
- **Docker & Docker Compose**: Containerization and deployment

### Dependencies

```
pandas
psycopg2-binary
pymysql
sqlalchemy
python-dotenv
numpy
pytest
flake8
pytest-mock
pytest-cov
apache-airflow
apache-airflow-providers-postgres
```

## Running the Project

### Prerequisites
- Docker and Docker Compose installed
- Git for cloning the repository

### Setup and Execution

1. **Clone the Repository**
   ```bash
   git clone https://github.com/yasserzs3/MLOps-HW1.git
   cd MLOps-HW1
   ```

2. **Start the Services**
   ```bash
   cd docker
   docker-compose up --build -d
   ```

   This command:
   - Builds the Docker images
   - Creates the required network
   - Sets up PostgreSQL database
   - Initializes Airflow with admin user
   - Starts the Airflow webserver and scheduler

3. **Access Airflow UI**
   - Open `http://localhost:8080` in your browser
   - Login with:
     - Username: admin
     - Password: admin

4. **Run the DAG**
   - Navigate to DAGs in the Airflow UI
   - Enable and trigger the `retail_sales_etl` DAG

5. **Stop the Services**
   ```bash
   docker-compose down
   ```

## Testing

Run the automated tests with:
```bash
pytest
```

For test coverage report:
```bash
pytest --cov=scripts
```

## Development

When developing locally:
1. Set up a virtual environment
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. Run linting to ensure code quality
   ```bash
   flake8 dags scripts tests
   ``` 