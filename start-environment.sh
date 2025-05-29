#!/bin/bash

echo "Starting Hadoop and Spark services..."
docker-compose up -d namenode datanode1 datanode2 spark-master

echo "Waiting for Spark master to be healthy..."
while true; do
  if curl -s http://localhost:8080 > /dev/null; then
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

echo "Checking and creating HDFS data directory..."
docker exec namenode hdfs dfs -mkdir -p /data

echo "Waiting for Jupyter to be ready..."
sleep 10

echo "Spark cluster is now ready with datasets loaded in HDFS!"
echo "Worker configuration: 6 cores and 6GB memory per worker"
echo "Datasets available in:"
echo "  - Local path: ./datasets"
echo "  - HDFS path: hdfs://namenode:9000/data/"
echo "  - Worker path: /opt/bitnami/spark/datasets"
echo "  - Jupyter path: /home/jovyan/datasets" 

chmod +x start-essential.sh
chmod +x init-hdfs.sh
chmod +x fix_hive_issue.sh
./start-essential.sh
./init-hdfs.sh
sleep 5
./fix_hive_issue.sh