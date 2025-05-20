#!/usr/bin/env python
# Script to test Spark's ability to access datasets

import os
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Test Dataset Access") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("Spark version:", spark.version)
print("Connected to:", spark.sparkContext.master)

# 1. Try to access the dataset from Jupyter's perspective
print("\nApproach 1: Testing dataset access via Jupyter container volume:")
try:
    df = spark.read.csv('/home/jovyan/datasets/cities.csv', header=True, inferSchema=True)
    print(f"SUCCESS: Read {df.count()} rows from Jupyter volume")
    print("Sample data:")
    df.show(5)
except Exception as e:
    print(f"ERROR: Failed to read dataset via Jupyter volume: {str(e)}")

# 2. Try to access from Spark worker's perspective
print("\nApproach 2: Testing dataset access via Spark container volume:")
try:
    df2 = spark.read.csv('file:///opt/bitnami/spark/datasets/cities.csv', header=True, inferSchema=True)
    print(f"SUCCESS: Read {df2.count()} rows from Spark volume")
    print("Sample data:")
    df2.show(5)
except Exception as e:
    print(f"ERROR: Failed to read dataset via Spark volume: {str(e)}")

print("\nTest complete!")
spark.stop() 