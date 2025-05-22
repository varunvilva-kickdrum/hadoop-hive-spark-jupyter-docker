#!/bin/bash

# Create required directories
mkdir -p ./airflow/logs
mkdir -p ./airflow/plugins
mkdir -p ./airflow/config
mkdir -p ./datasets/output

# Set proper permissions for Airflow directories
chmod -R 777 ./airflow/logs
chmod -R 777 ./airflow/plugins
chmod -R 777 ./airflow/config
chmod -R 777 ./airflow/dags

# Set Airflow UID to avoid permission issues
export AIRFLOW_UID=$(id -u)

# Check if the bigdata_network exists, if not create it
if ! docker network inspect bigdata_network &>/dev/null; then
  echo "Creating bigdata_network..."
  docker network create bigdata_network
fi

# Start Airflow services using docker-compose
docker-compose -f airflow/docker-compose-airflow.yml up -d

echo "Airflow services started."
echo "Airflow UI will be available at: http://localhost:8081"
echo "Flower UI will be available at: http://localhost:5555"
echo "Default credentials: airflow / airflow" 