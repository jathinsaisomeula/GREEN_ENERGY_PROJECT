from kafka import KafkaConsumer
import json
import os
import time

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
print(f"Consumer attempting to connect to KAFKA_BROKER: {KAFKA_BROKER}")
TOPIC_NAME = 'GREEN_ENERGY_DATA_TOPIC' # Matching producer's topic name
GROUP_ID = 'GREEN_ENERGY_CONSUMER_GROUP' # Group ID for Green Energy Data

def deserialize_data(data_bytes):
    """Deserialize JSON bytes from Kafka message."""
    decoded_string = data_bytes.decode('utf-8')
    return json.loads(decoded_string)

def consume_messages():
    consumer = None
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest', # Start reading from the beginning of the topic
            enable_auto_commit=True,
            group_id=GROUP_ID,
            value_deserializer=deserialize_data,
            consumer_timeout_ms=1000 # Stop after 1 second if no more messages are available
        )
        print(f"Subscribed to topic: '{TOPIC_NAME}' in group '{GROUP_ID}'. Waiting for messages... (Press Ctrl+C to stop)")

        for message in consumer:
            energy_record = message.value # 'message.value' is now a Python dictionary
            print(f"--- Received Green Energy Record (Offset: {message.offset}, Partition: {message.partition}) ---")
            # Print key fields with their precise names
            print(f"  Date: {energy_record.get('Date', 'N/A')}")
            print(f"  Country: {energy_record.get('Country', 'N/A')}")
            print(f"  Year: {energy_record.get('Year', 'N/A')}")
            print(f"  Population (Millions): {energy_record.get('Population_Millions', 'N/A')}")
            print(f"  GDP (Billion USD): {energy_record.get('GDP_Billion_USD', 'N/A')}")
            print(f"  CO2 Emissions (Million Tons): {energy_record.get('CO2_Emissions_Million_Tons', 'N/A')}")
            print(f"  Renewable Energy Share: {energy_record.get('Renewable_Energy_Share_Percentage', 'N/A')}%")
            print(f"  Solar Generation (TWh): {energy_record.get('Solar_Generation_TWh', 'N/A')}")
            print(f"  Wind Generation (TWh): {energy_record.get('Wind_Generation_TWh', 'N/A')}")
            print(f"  Investment in Renewables (Billion USD): ${energy_record.get('Investment_in_Renewables_Billion_USD', 'N/A')}")
            print(f"  Green Jobs Created: {energy_record.get('Green_Jobs_Created', 'N/A')}")
            print(f"  Sustainability Score: {energy_record.get('Sustainability_Score', 'N/A')}")
            print("-" * 40)
            time.sleep(0.05) # Small delay to simulate processing

    except KeyboardInterrupt:
        print("\nConsumer gracefully stopped by user.")
    except Exception as e:
        print(f"Error consuming messages: {e}")
        print("Please ensure your Kafka container is running, the topic exists, and the broker address is correct.")
        print(f"If you are on macOS/Windows and Kafka is in Docker, confirm Docker Desktop is running and port {KAFKA_BROKER.split(':')[-1]} is correctly mapped.")
    finally:
        if consumer:
            consumer.close()
            print("Consumer connection closed.")

if __name__ == "__main__":
    consume_messages()