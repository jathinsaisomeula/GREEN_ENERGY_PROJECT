# Use an official Python runtime as a parent image
FROM python:3.11-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application's source code to /app
# This includes producer.py, consumer.py, spark_processor.py, and supply_chain_data.csv
COPY . .

# No need for CMD here, as docker-compose will override it with 'sleep infinity'
