#!/bin/bash

echo "Waiting for Spark master to be healthy..."
while true; do
  if docker exec spark-master curl -s http://spark-master:8080 > /dev/null; then
    echo "Spark master is ready!"
    break
  else
    echo "Waiting for Spark master to be healthy..."
    sleep 5
  fi
done

echo "Starting Spark workers..."
docker-compose up -d spark-worker1 spark-worker2

echo "Waiting for Spark workers to initialize..."
sleep 15

echo "Starting Jupyter notebook..."
docker-compose up -d jupyter

echo "Spark cluster is now ready!" 