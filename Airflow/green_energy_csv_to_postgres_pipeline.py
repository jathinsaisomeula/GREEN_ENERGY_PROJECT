from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define CSV file details
# This is the EXACT filename confirmed from your 'ls -F' output.
CSV_FILENAME = 'custom_green_energy_monthly_data_v2.csv'
# This path is INSIDE the Docker container, mapped from your host machine's project directory
CSV_FULL_PATH = '/usr/local/airflow_data/' + CSV_FILENAME

def extract_transform_load_to_postgres():
    logger.info(f"Attempting to read CSV from: {CSV_FULL_PATH}")
    try:
        # Read the CSV file
        df = pd.read_csv(CSV_FULL_PATH)
        logger.info(f"Successfully read {len(df)} rows from CSV.")

        # Basic transformation: ensure 'Date' is datetime and 'Total_Energy_Consumption_TWh' is numeric
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        else:
            logger.warning("Column 'Date' not found. Skipping datetime conversion for this column.")

        # Check for 'Total_Energy_Consumption_TWh' and convert to numeric
        if 'Total_Energy_Consumption_TWh' in df.columns:
            df['Total_Energy_Consumption_TWh'] = pd.to_numeric(df['Total_Energy_Consumption_TWh'], errors='coerce')
        else:
            logger.warning("Column 'Total_Energy_Consumption_TWh' not found. Skipping numeric conversion for this column.")
        
        # Drop rows where critical columns became NaN after coercion
        # We'll make 'Date' and 'Total_Energy_Consumption_TWh' the critical columns for dropping NaNs
        # Add a check to ensure these columns exist before attempting dropna
        columns_to_check = []
        if 'Date' in df.columns:
            columns_to_check.append('Date')
        if 'Total_Energy_Consumption_TWh' in df.columns:
            columns_to_check.append('Total_Energy_Consumption_TWh')

        if columns_to_check:
            df.dropna(subset=columns_to_check, inplace=True)
            logger.info(f"After cleaning, {len(df)} rows remain.")
        else:
            logger.warning("Neither 'Date' nor 'Total_Energy_Consumption_TWh' columns found. Skipping NaN drop.")


        # PostgreSQL connection string using the service name 'postgres' defined in docker-compose.yml
        # Ensure these credentials match the POSTGRES_USER and POSTGRES_PASSWORD in your docker-compose.yml
        db_connection_str = 'postgresql+psycopg2://jathinsai:jathinsai143@postgres:5432/green_energy_db'
        db_connection = create_engine(db_connection_str)
        logger.info("PostgreSQL engine created.")

        # Load data into PostgreSQL
        table_name = 'monthly_energy_data'
        df.to_sql(table_name, db_connection, if_exists='replace', index=False)
        logger.info(f"Data successfully loaded into PostgreSQL table '{table_name}'.")

        # Verify data by reading back a sample
        # Adapt columns for verification based on actual CSV columns
        sample_columns = ['Date', 'Country', 'Total_Energy_Consumption_TWh']
        existing_sample_columns = [col for col in sample_columns if col in df.columns]
        
        if existing_sample_columns:
            sample_df = pd.read_sql_table(table_name, db_connection, columns=existing_sample_columns).head(5)
            logger.info(f"Sample data from '{table_name}':\n{sample_df.to_string()}")
        else:
            logger.warning("No suitable columns found to verify sample data from PostgreSQL.")

    except FileNotFoundError:
        logger.error(f"Error: CSV file not found at {CSV_FULL_PATH}. Please ensure the file exists and the Docker volume mount is correct.")
        raise
    except Exception as e:
        logger.error(f"An error occurred during data processing or loading: {e}", exc_info=True)
        raise

default_args = {
    'owner': 'jathinsai',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='green_energy_csv_to_postgres_pipeline',
    default_args=default_args,
    description='Loads Green Energy CSV data into PostgreSQL and verifies its content.',
    schedule_interval=None, # Run manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['green_energy', 'postgres', 'csv'],
) as dag:
    start_green_energy_pipeline = BashOperator(
        task_id='start_green_energy_pipeline',
        bash_command='echo "Starting Green Energy Data Pipeline..."',
    )

    extract_csv_and_load_to_postgres = PythonOperator(
        task_id='extract_csv_and_load_to_postgres',
        python_callable=extract_transform_load_to_postgres,
    )

    end_green_energy_pipeline = BashOperator(
        task_id='end_green_energy_pipeline',
        bash_command='echo "Green Energy Data Pipeline Finished."',
    )

    start_green_energy_pipeline >> extract_csv_and_load_to_postgres >> end_green_energy_pipeline
