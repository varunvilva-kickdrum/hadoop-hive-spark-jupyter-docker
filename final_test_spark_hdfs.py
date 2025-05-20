#!/usr/bin/env python
# Final Test Script for Spark Dataset Access
# This script tests both direct file access and HDFS access

import os
import sys
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Create a properly configured Spark session
spark = SparkSession.builder \
    .appName("Dataset Access Test") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.host", "jupyter") \
    .getOrCreate()

print("=============================================")
print("SPARK CONFIGURATION TEST")
print("=============================================")
print(f"Spark version: {spark.version}")
print(f"Spark master: {spark.sparkContext.master}")
print(f"Application ID: {spark.sparkContext.applicationId}")
print(f"Driver host: {spark.conf.get('spark.driver.host')}")

# Test method 1: Direct file access using driver path
print("\n=============================================")
print("TEST 1: SHARED VOLUME ACCESS (DRIVER PATH)")
print("=============================================")

try:
    # The path as seen from the driver (Jupyter container)
    driver_path = "/home/jovyan/datasets/cities.csv"
    
    if os.path.exists(driver_path):
        print(f"File exists on driver at: {driver_path}")
        df1 = spark.read.option("header", "true").csv(driver_path)
        count = df1.count()
        print(f"SUCCESS: Read {count} rows")
        print("Sample data:")
        df1.show(5)
    else:
        print(f"ERROR: File not found at {driver_path}")
except Exception as e:
    print(f"ERROR: {str(e)}")

# Test method 2: Direct file access using explicit file:// protocol with worker path
print("\n=============================================")
print("TEST 2: SHARED VOLUME ACCESS (WORKER PATH)")
print("=============================================")

try:
    # The path as seen from the workers
    worker_path = "file:///opt/bitnami/spark/datasets/cities.csv"
    df2 = spark.read.option("header", "true").csv(worker_path)
    count = df2.count()
    print(f"SUCCESS: Read {count} rows")
    print("Sample data:")
    df2.show(5)
except Exception as e:
    print(f"ERROR: {str(e)}")

# Test method 3: HDFS access
print("\n=============================================")
print("TEST 3: HDFS ACCESS")
print("=============================================")

try:
    # Try to access via HDFS if available
    hdfs_path = "hdfs://namenode:9000/data/cities.csv"
    df3 = spark.read.option("header", "true").csv(hdfs_path)
    count = df3.count()
    print(f"SUCCESS: Read {count} rows from HDFS")
    print("Sample data:")
    df3.show(5)
except Exception as e:
    print(f"ERROR: HDFS access failed: {str(e)}")
    print("This is expected if HDFS is not configured properly or the file hasn't been uploaded to HDFS")

print("\n=============================================")
print("TEST COMPLETE")
print("=============================================")
spark.stop() 