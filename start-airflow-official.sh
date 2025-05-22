#!/bin/bash

# Create required directories
mkdir -p ./airflow/dags
mkdir -p ./airflow/logs
mkdir -p ./airflow/plugins

# Set proper permissions
chmod -R 777 ./airflow/logs
chmod -R 777 ./airflow/plugins
chmod -R 777 ./airflow/dags

# Set Airflow UID to avoid permission issues
export AIRFLOW_UID=$(id -u)

# Check if the bigdata_network exists, if not create it
if ! docker network inspect bigdata_network &>/dev/null; then
  echo "Creating bigdata_network..."
  docker network create bigdata_network
fi

cd airflow

# Stop any existing Airflow containers
docker-compose -f docker-compose-official.yml down

# Start Airflow services using docker-compose
docker-compose -f docker-compose-official.yml up airflow-init
docker-compose -f docker-compose-official.yml up -d --remove-orphans

echo "Airflow services started."
echo "Airflow UI will be available at: http://localhost:9090 or via Nginx proxy at http://localhost:8085"
echo "Flower UI will be available at: http://localhost:5555"
echo "Default credentials: airflow / airflow" 