#!/bin/bash

# Initialize the database
airflow db migrate

# Create the admin user if it doesn't exist
airflow users create \
  --role Admin \
  --username airflow \
  --password airflow \
  --email airflow@airflow.com \
  --firstname airflow \
  --lastname airflow || true

# Start the Airflow process
exec airflow "$@"
