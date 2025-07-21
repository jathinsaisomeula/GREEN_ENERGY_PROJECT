# Use the official Airflow image as the base
FROM apache/airflow:2.9.2-python3.11

# Install necessary Python packages
# Use the Airflow user to install packages to avoid permission issues
USER airflow
RUN pip install --no-cache-dir pandas psycopg2-binary