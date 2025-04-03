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

#### 2. Transform Task
- **Implementation**: `scripts/transformation.py`
- **Purpose**: Processes and aggregates sales data
- **Key Operations**:
  - Combines online and in-store sales data
  - Cleans invalid or null entries
  - Aggregates sales by product_id
  - Calculates daily totals and metrics
- **Data Validation**:
  - Type checking and conversion
  - Handling of missing values
  - Data quality assurance

#### 3. Load Task
- **Implementation**: `scripts/loading.py`
- **Purpose**: Loads processed data into target database
- **Features**:
  - Creates or updates sales_summary table
  - Implements batch loading for performance
  - Ensures data integrity with transaction handling
  - Error recovery mechanisms

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