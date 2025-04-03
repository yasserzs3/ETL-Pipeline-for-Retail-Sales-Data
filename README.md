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
  - Performs complete aggregation by product_id to calculate total quantities and amounts
  - Prepares final dataset for loading with no further aggregation needed
- **Data Validation**:
  - Type checking and conversion
  - Handling of missing values
  - Data quality assurance
- **Key Functions**:
  - `transform_data()`: Main function that orchestrates the entire transformation process
  - `load_and_validate_data()`: Loads JSON strings from XCom and converts to DataFrames
  - `convert_numeric_columns()`: Converts string columns to numeric types using pandas to_numeric
  - `clean_data()`: Removes rows with null values and filters out records with non-positive quantities or amounts
  - `aggregate_sales()`: Performs full aggregation by product_id only, calculating total quantity and total sale amount

#### 3. Load Task
- **Implementation**: `scripts/loading.py`
- **Purpose**: Loads processed data into target database
- **Features**:
  - Creates or updates sales_summary table
  - Truncates existing data before inserting new data
  - Loads pre-aggregated data directly with no further processing
  - Maintains database integrity with transaction handling
- **Key Functions**:
  - `load_data()`: Loads the already-aggregated data to both CSV and database
  - `validate_dataframe()`: Performs final validation on data structure and values
  - Uses TRUNCATE TABLE to clear existing data before each load
  - Overwrites existing CSV output file rather than merging with it
  - No additional aggregation is performed in this step

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

### Checking Results

After running the DAG, the same aggregated data is stored in both the PostgreSQL database and a CSV output file.

#### Database Results

To check the data in the PostgreSQL database:

```bash
# Access the PostgreSQL container
docker exec -it MLops-HW1-postgres bash

# Connect to the database
psql -U airflow -d airflow

# View the sales summary table
SELECT * FROM sales_summary;

# Exit
\q
exit
```

#### CSV Output File

The same data is also stored in a CSV file. To view the CSV output file:

```bash
# Access the Airflow container
docker exec -it MLops-HW1 bash

# Navigate to the output directory
cd /opt/airflow/data/output

# View the CSV file
cat sales_summary.csv

# Exit
exit
```

Both the database table and the CSV file contain identical data, as they are generated from the same processed dataset during the load phase.

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

## Challenges and Reflections

### Airflow Setup Challenges

**Docker Configuration:** Configuring Airflow in Docker required balancing numerous settings while ensuring proper communication between containers. Service dependencies and startup ordering were particularly tricky to manage.

**Volume Management:** Setting up proper volume mounts for DAGs, scripts, and data directories was challenging. Permissions issues occasionally arose when containers tried to write to mounted volumes.

**Connection Management:** Establishing stable connections between Airflow and PostgreSQL required careful configuration. Environment variables needed to be consistent across services for seamless integration.

### Pipeline Design Challenges

**Data Accumulation Issue:** Earlier versions of the pipeline inadvertently accumulated data across DAG runs instead of replacing it. This was fixed by implementing table truncation and proper upsert patterns.

**Separation of Responsibilities:** Initially, both transform and load steps performed aggregation, causing data duplication. We refactored to ensure all aggregation happens in the transform step only.

**Error Handling:** Implementing robust error handling required careful transaction management and validation at each stage. Consistent logging patterns were crucial for troubleshooting. 