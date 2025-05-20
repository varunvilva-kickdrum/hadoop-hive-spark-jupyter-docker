#!/usr/bin/env python
# HDFS and Spark Integration Demo
# This script demonstrates reading data from HDFS in Spark

import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session with proper configuration
spark = SparkSession.builder \
    .appName("HDFS Demo") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.host", "jupyter") \
    .config("spark.ui.enabled", "true") \
    .getOrCreate()

# Print connection info
print(f"Spark version: {spark.version}")
print(f"Connected to master: {spark.sparkContext.master}")
print(f"Spark UI available at: http://localhost:4040")

# List files in HDFS
print("\nFiles available in HDFS:")
hadoop_files = spark.read.format("text").load("hdfs://namenode:9000/data/*").collect()
for f in hadoop_files:
    print(f"- {f}")

# Check if a specific file exists in HDFS
file_name = "cities.csv"  # Replace with your actual file name
import subprocess
result = subprocess.run(f"hdfs dfs -ls /data/{file_name}", shell=True, capture_output=True, text=True)
file_exists = result.returncode == 0

if file_exists:
    print(f"\nReading {file_name} from HDFS...")
    
    # Read CSV from HDFS
    df = spark.read.option("header", "true") \
              .option("inferSchema", "true") \
              .csv(f"hdfs://namenode:9000/data/{file_name}")
    
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
    
    print("\nDataFrame successfully read from HDFS!")
    print("You can now access the Spark UI at http://localhost:4040 to see the execution details")
else:
    print(f"\nFile {file_name} not found in HDFS.")
    print("Please run load_to_hdfs.py to upload your datasets first.")
    print("Or replace 'cities.csv' with a file that exists in your HDFS /data directory.")

input("\nPress Enter to exit...") 