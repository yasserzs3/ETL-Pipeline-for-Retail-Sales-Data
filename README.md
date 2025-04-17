# ETL Pipeline for Retail Sales Data

## Table of Contents
- [Project Overview](#project-overview)
- [Project Structure](#project-structure)
- [DAG Workflow](#dag-workflow)
  - [Extract Task](#1-extract-task)
  - [Transform Task](#2-transform-task)
  - [Load Task](#3-load-task)
- [Implementation Details](#implementation-details)
  - [Technologies Used](#technologies-used)
  - [Dependencies](#dependencies)
- [Deployment and Usage](#deployment-and-usage)
  - [Basic Setup with Docker Compose](#basic-setup-with-docker-compose)
  - [Detailed Docker Compose Deployment](#detailed-docker-compose-deployment)
  - [Docker Swarm Deployment](#docker-swarm-deployment)
  - [Scaling and Load Balancing with Docker Swarm](#scaling-and-load-balancing-with-docker-swarm)
  - [Checking Results](#checking-results)
- [Testing and Development](#testing-and-development)
  - [Testing](#testing)
  - [Development Environment](#development)
- [Challenges and Reflections](#challenges-and-reflections)
  - [Airflow Setup Challenges](#airflow-setup-challenges)
  - [Docker Swarm Deployment Challenges](#docker-swarm-deployment-challenges)
  - [Pipeline Design Challenges](#pipeline-design-challenges)

## Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline for processing daily retail sales data from multiple sources using Apache Airflow. The pipeline combines online sales data from a PostgreSQL database with in-store sales from CSV files, processes the combined data, and loads the results into a centralized MySQL database.

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
- **Purpose**: Loads processed data into target MySQL database
- **Features**:
  - Creates or updates sales_summary table
  - Truncates existing data before inserting new data
  - Loads pre-aggregated data directly with no further processing
  - Maintains database integrity with transaction handling
- **Key Functions**:
  - `load_data()`: Loads the already-aggregated data to both CSV and MySQL database
  - `validate_dataframe()`: Performs final validation on data structure and values
  - Uses TRUNCATE TABLE to clear existing data before each load
  - Overwrites existing CSV output file rather than merging with it
  - No additional aggregation is performed in this step

## Implementation Details

### Technologies Used

- **Apache Airflow**: Workflow orchestration
- **PostgreSQL**: Source database for online sales
- **MySQL**: Target database for aggregated sales
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

## Deployment and Usage

### Prerequisites
- Docker and Docker Compose installed
- Git for cloning the repository

### Basic Setup with Docker Compose

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

### Docker Swarm Deployment

For production deployments or to scale the application, you can use Docker Swarm for orchestration and load balancing.

#### Building Airflow Image

Before deploying with Docker Swarm, you need to build the Airflow image locally.

1. **Build the Airflow Image**
   ```bash
   cd docker
   docker build -t mlops-airflow:latest .
   ```

2. **Use the Image in Your Stack Configuration**
   
   Make sure your `docker/docker-stack.yml` references this image:
   ```yaml
   services:
     webserver:
       image: mlops-airflow:latest
       # other configuration...
   ```

#### Setting Up Docker Swarm

1. **Initialize Docker Swarm** (if not already done)
   ```bash
   docker swarm init
   ```

2. **Deploy the Stack**
   ```bash
   docker stack deploy -c docker/docker-stack.yml mlops-stack
   ```

   This command:
   - Creates an overlay network for service communication
   - Deploys PostgreSQL and MySQL databases with persistent volumes
   - Sets up the Airflow initialization service to create connections
   - Deploys multiple instances of the webserver and scheduler for high availability

3. **Verify Stack Deployment**
   ```bash
   docker service ls
   ```

   You should see the following services:
   - mlops-stack_postgres
   - mlops-stack_mysql
   - mlops-stack_airflow-init
   - mlops-stack_webserver (with multiple replicas)
   - mlops-stack_scheduler (with multiple replicas)

4. **Wait for Initialization to Complete**
   ```bash
   docker service logs mlops-stack_airflow-init --follow
   ```

   Wait until you see output indicating successful initialization:
   ```
   Airflow initialization complete!
   ```
   
   The initialization service creates the necessary database tables, connections, and default user. The service will exit with code 0 when complete.
   
   You can verify initialization is complete with:
   ```bash
   docker service ps mlops-stack_airflow-init
   ```
   
   Look for "Shutdown" or "Complete" status in the output.

5. **Access Airflow UI**
   - Open `http://localhost:8080` in your browser
   - Login with:
     - Username: admin
     - Password: admin

6. **Scaling Services**
   To scale specific services (for example, to add more webserver instances):
   ```bash
   docker service scale mlops-stack_webserver=3
   ```

7. **Remove the Stack**
   When you're done, you can remove the entire stack:
   ```bash
   docker stack rm mlops-stack
   ```

8. **Leave Swarm Mode** (optional)
   ```bash
   docker swarm leave --force
   ```

#### Key Benefits of Docker Swarm Deployment

- **High Availability**: Multiple replicas ensure continuous operation
- **Load Balancing**: Requests are distributed across service instances
- **Service Discovery**: Built-in DNS resolution for inter-service communication
- **Rolling Updates**: Update services without downtime
- **Self-Healing**: Automatically restarts failed containers

#### Troubleshooting Swarm Deployment

- **Check Service Logs**
  ```bash
  docker service logs mlops-stack_webserver
  ```

- **Inspect Service**
  ```bash
  docker service inspect mlops-stack_webserver
  ```

- **View Running Tasks**
  ```bash
  docker service ps mlops-stack_webserver
  ```

- **Check Network Connectivity**
  ```bash
  docker network inspect mlops-stack_airflow-network
  ```

### Scaling and Load Balancing with Docker Swarm

For production deployments with high availability and scalability requirements, Docker Swarm provides built-in load balancing, service discovery, and scaling capabilities.

Key scaling features in Docker Swarm:

1. **Service Scaling**
   ```bash
   docker service scale mlops-stack_webserver=3
   ```

2. **Load Balancing**
   Docker Swarm provides automatic load balancing across service replicas without additional configuration.

3. **High Availability**
   Configure services with multiple replicas in the docker-stack.yml:
   ```yaml
   services:
     webserver:
       image: mlops-airflow:latest
       deploy:
         replicas: 3
         restart_policy:
           condition: on-failure
   ```

4. **Resource Constraints**
   ```yaml
   services:
     webserver:
       deploy:
         resources:
           limits:
             cpus: '1'
             memory: 1G
           reservations:
             cpus: '0.5'
             memory: 512M
   ```

5. **Rolling Updates**
   ```yaml
   services:
     webserver:
       deploy:
         update_config:
           parallelism: 1
           delay: 10s
           order: start-first
   ```

Refer to the Docker Swarm Deployment section for complete instructions on setting up and managing Docker Swarm deployments.

### Detailed Docker Compose Deployment

This section provides comprehensive deployment instructions using Docker Compose for development and testing environments.

#### Building Docker Images

1. **Build All Services at Once**
   ```bash
   cd docker
   docker-compose build
   ```

2. **Build with No Cache** (for troubleshooting)
   ```bash
   docker-compose build --no-cache
   ```

#### Deploying the Application

1. **Start in Detached Mode**
   ```bash
   docker-compose up -d
   ```

2. **Start with Build Flag** (build images before starting)
   ```bash
   docker-compose up -d --build
   ```

#### Monitoring Deployment

1. **View Running Containers**
   ```bash
   docker-compose ps
   ```

2. **View Service Logs**
   ```bash
   # All logs
   docker-compose logs
   
   # Specific service logs with follow option
   docker-compose logs -f [service_name]
   
   # Tail specific number of lines
   docker-compose logs --tail=100 [service_name]
   ```

3. **Checking Service Health**
   ```bash
   # Check database connection
   docker-compose exec [database_service] [database_check_command]
   
   # Check web service
   curl http://localhost:8080
   ```

#### Interacting with Services

1. **Execute Commands in Containers**
   ```bash
   # Run bash in a container
   docker-compose exec [service_name] bash
   
   # Run Airflow commands
   docker-compose exec [airflow_service] airflow info
   docker-compose exec [airflow_service] airflow dags list
   docker-compose exec [airflow_service] airflow dags trigger retail_sales_etl
   ```

2. **Access Database Shells**
   ```bash
   # Connect to database
   docker-compose exec [database_service] [database_connection_command]
   ```

#### Managing Deployment

1. **Stop Services**
   ```bash
   docker-compose stop
   ```

2. **Start Stopped Services**
   ```bash
   docker-compose start
   ```

3. **Restart Services**
   ```bash
   docker-compose restart [service_name]
   ```

4. **Remove Containers while Preserving Volumes**
   ```bash
   docker-compose down
   ```

5. **Complete Cleanup** (including volumes)
   ```bash
   docker-compose down -v
   ```

### Checking Results

After running the DAG, the same aggregated data is stored in both the MySQL database and a CSV output file.

#### Database Results

To check the data in the MySQL database:

```bash
# Access the MySQL container
docker exec -it MLops-HW1-mysql bash

# Connect to the database
mysql -u airflow -pairflow airflow

# View the sales summary table
SELECT * FROM sales_summary;

# Exit
exit
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

## Testing and Development

### Testing

Run the automated tests with:
```bash
pytest
```

For test coverage report:
```bash
pytest --cov=scripts
```

### Development

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

### Docker Swarm Deployment Challenges

**Airflow Component Orchestration:** Configuring Airflow's multiple components (webserver, scheduler, worker, init) for Docker Swarm presented significant challenges. Each component required specific environment variables and startup conditions.

**Initialization Sequence:** The initialization service needed to complete successfully before other services could start properly. Creating a reliable startup sequence with proper dependency management was complex.

**Service Discovery:** Ensuring that Airflow components could discover each other in the Swarm network required careful configuration of network aliases and DNS settings.

**Persistent Storage:** Managing persistent volumes across Swarm nodes for databases and Airflow's metadata was challenging, especially when scaling services across multiple nodes.

**Load Balancing Configuration:** While Swarm provides built-in load balancing, configuring it optimally for Airflow's webserver required additional testing to ensure session persistence and proper request routing.

### Pipeline Design Challenges

**Data Accumulation Issue:** Earlier versions of the pipeline inadvertently accumulated data across DAG runs instead of replacing it. This was fixed by implementing table truncation and proper upsert patterns.

**Separation of Responsibilities:** Initially, both transform and load steps performed aggregation, causing data duplication. We refactored to ensure all aggregation happens in the transform step only.

**Error Handling:** Implementing robust error handling required careful transaction management and validation at each stage. Consistent logging patterns were crucial for troubleshooting.