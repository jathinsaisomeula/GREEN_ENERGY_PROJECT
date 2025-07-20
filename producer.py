import csv
import json
import time
import os
import uuid
from kafka import KafkaProducer

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = 'GREEN_ENERGY_DATA_TOPIC'
FILE_PATH = 'custom_green_energy_monthly_data_v2.csv' # Ensure this path is correct

# Define expected data types for conversion, matching your CSV headers
# Columns not listed here will be treated as strings by default or 'N/A' if missing
data_types = {
    'Year': int,
    'Month': int,
    'Population_Millions': float,
    'GDP_Billion_USD': float,
    'GDP_Per_Capita_USD': float,
    'CO2_Emissions_Million_Tons': float,
    'Carbon_Footprint_Per_Capita_Tons': float,
    'Air_Quality_Index_Avg': float,
    'Waste_Recycling_Rate_Percentage': float,
    'Deforestation_Rate_Hectares_Per_Year': float,
    'Total_Energy_Consumption_TWh': float,
    'Renewable_Energy_Share_Percentage': float,
    'Solar_Generation_TWh': float,
    'Wind_Generation_TWh': float,
    'Hydro_Generation_TWh': float,
    'Other_Renewable_Generation_TWh': float,
    'Fossil_Fuel_Generation_TWh': float,
    'Installed_Renewable_Capacity_GW': float,
    'EV_Sales_Percentage_of_New_Cars': float,
    'Green_Policy_Strength_Index': float,
    'Policy_Count_Last_5_Yrs': int,
    'Investment_in_Renewables_Billion_USD': float,
    'Renewable_Energy_Subsidies_Billion_USD': float,
    'Green_Jobs_Created': int,
    'Average_Electricity_Price_USD_per_kWh': float,
    'Green_Marketing_Spend_Per_Capita_USD': float,
    'Public_Green_Awareness_Score': float,
    'Sustainability_Score': float
}

def serialize_data(data):
    """Serialize Python dictionary to JSON bytes."""
    return json.dumps(data).encode('utf-8')

def produce_messages():
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=serialize_data,
            linger_ms=100 # Add a small buffer to send messages in batches
        )
        print(f"Producer attempting to connect to KAFKA_BROKER: {KAFKA_BROKER}")
        print(f"Producing messages to topic: '{TOPIC_NAME}' from file: '{FILE_PATH}'")

        # Give Kafka some time to start up and topic to be created if not exists
        time.sleep(5)

        with open(FILE_PATH, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            for i, row in enumerate(csv_reader):
                processed_row = {}
                processed_row['id'] = str(uuid.uuid4()) # Add a unique ID for each record

                for col_name, value in row.items():
                    # Strip whitespace from column names for robust matching
                    clean_col_name = col_name.strip()
                    try:
                        # Convert to target type based on data_types map
                        if clean_col_name in data_types:
                            # Handle empty strings for numerical conversions
                            if value.strip() == '':
                                processed_row[clean_col_name] = None # Or 0, or skip, based on preference
                            else:
                                processed_row[clean_col_name] = data_types[clean_col_name](value)
                        else:
                            processed_row[clean_col_name] = value.strip() # Keep as string
                    except ValueError:
                        print(f"Warning: Could not convert '{value}' for column '{clean_col_name}' to {data_types.get(clean_col_name, 'string')}. Storing as original string or None.")
                        processed_row[clean_col_name] = value.strip() if value.strip() != '' else None # Store original if conversion fails, or None for empty

                future = producer.send(TOPIC_NAME, processed_row)
                record_metadata = future.get(timeout=10) # Blocks until sent
                print(f"Sent record {i+1} to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")

                time.sleep(0.1) # Simulate real-time stream

    except FileNotFoundError:
        print(f"Error: The file '{FILE_PATH}' was not found. Please ensure it's in the same directory as this script.")
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Please ensure your Kafka container is running and accessible.")
    finally:
        if producer:
            producer.flush() # Ensure all outstanding messages are sent
            producer.close()
            print("Producer closed.")

if __name__ == "__main__":
    produce_messages()