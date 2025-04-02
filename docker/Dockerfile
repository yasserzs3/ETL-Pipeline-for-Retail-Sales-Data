FROM apache/airflow:2.5.0

# Install dependencies
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy project files
COPY dags/ ${AIRFLOW_HOME}/dags/
COPY scripts/ ${AIRFLOW_HOME}/scripts/
COPY data/ ${AIRFLOW_HOME}/data/

# Set environment variables
ENV PYTHONPATH=${PYTHONPATH}:${AIRFLOW_HOME}/scripts

# Set correct permissions
USER root
RUN chown -R airflow:root ${AIRFLOW_HOME}
USER airflow 