#!/usr/bin/env python
# Shared Datasets Demo
# This script demonstrates using shared volume mounts across all Spark containers

import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session with proper configuration
spark = SparkSession.builder \
    .appName("Shared Datasets Demo") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.host", "jupyter") \
    .config("spark.ui.enabled", "true") \
    .getOrCreate()

# Print connection info
print(f"Spark version: {spark.version}")
print(f"Connected to master: {spark.sparkContext.master}")
print(f"Spark UI available at: http://localhost:4040")

# List the contents of the datasets directory
datasets_path = "/home/jovyan/datasets"
print(f"\nContents of {datasets_path}:")
for filename in os.listdir(datasets_path):
    print(f"- {filename}")

# Specify a file to read
file_name = "cities.csv"  # Replace with an actual file in your datasets folder
file_path = os.path.join(datasets_path, file_name)

if os.path.exists(file_path):
    print(f"\nReading {file_name} from dataset directory...")
    
    # This path will also be accessible to worker nodes through the shared volume
    # path for workers will be /opt/bitnami/spark/datasets/cities.csv
    
    # Read with the local path - will be sent to workers
    df = spark.read.option("header", "true") \
              .option("inferSchema", "true") \
              .csv(file_path)
    
    print(f"DataFrame schema:")
    df.printSchema()
    
    print(f"\nSample data:")
    df.show(5)
    
    # Perform some operations
    print(f"\nRow count: {df.count()}")
    
    # Cache to keep UI active
    df.cache()
    
    # Execute some transformations
    if "population" in [f.name.lower() for f in df.schema.fields]:
        print("\nTop 5 cities by population:")
        df.orderBy(col("population").desc()).show(5)
    
    print("\nDataFrame successfully read from shared dataset directory!")
    print("You can now access the Spark UI at http://localhost:4040 to see the execution details")
    
    # Alternate direct access using mounted path in workers
    print("\nReading same file using worker-accessible path...")
    worker_path = "/opt/bitnami/spark/datasets/cities.csv"
    
    # This path is directly accessible to worker nodes
    df2 = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .csv(f"file://{worker_path}")
    
    print(f"Row count from worker path: {df2.count()}")
    
else:
    print(f"\nFile {file_name} not found in datasets directory.")
    print(f"Please place some CSV files in the datasets directory: {datasets_path}")

input("\nPress Enter to exit...") 