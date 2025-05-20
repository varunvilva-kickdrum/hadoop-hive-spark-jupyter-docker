#!/bin/bash

echo "Starting essential services first..."
docker-compose up -d postgres namenode

echo "Waiting for postgres and namenode to be healthy..."
sleep 15

echo "Starting Hadoop and Hive services..."
docker-compose up -d datanode1 datanode2 hive

echo "Waiting for Hadoop cluster to stabilize..."
sleep 15

echo "Starting Spark cluster..."
docker-compose up -d spark-master

echo "Waiting for Spark master to initialize..."
sleep 20

echo "Starting Spark workers and Jupyter..."
docker-compose up -d

echo "Environment startup complete!" 