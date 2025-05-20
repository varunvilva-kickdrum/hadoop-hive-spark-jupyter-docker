#!/usr/bin/env python
# Dataset Access and Spark UI Demo
# This script demonstrates how to properly access datasets and view the Spark UI.

import os
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Create a Spark session with proper configuration
spark = SparkSession.builder \
    .appName("Dataset Demo") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.host", "jupyter") \
    .config("spark.ui.enabled", "true") \
    .getOrCreate()

# Print connection info
print(f"Spark version: {spark.version}")
print(f"Connected to master: {spark.sparkContext.master}")
print(f"Spark UI available at: http://localhost:4040")

# Check current working directory
print("Current working directory:", os.getcwd())

# List contents of the datasets directory
print("Contents of datasets directory:")
os.system("ls -la /home/jovyan/datasets")

# Method 1: Load data using absolute path
print("\nLoading data using absolute path:")
try:
    # Replace 'cities.csv' with your actual file name
    df1 = spark.read.csv('file:///home/jovyan/datasets/cities.csv', header=True, inferSchema=True)
    print("Method 1 (absolute path) succeeded!")
    df1.show(5)
except Exception as e:
    print(f"Method 1 failed: {str(e)}")

# Method 2: Load data using relative path
print("\nLoading data using relative path:")
try:
    # Replace 'cities.csv' with your actual file name
    df2 = spark.read.csv('../datasets/cities.csv', header=True, inferSchema=True)
    print("Method 2 (relative path) succeeded!")
    df2.show(5)
except Exception as e:
    print(f"Method 2 failed: {str(e)}")

# Create and cache a DataFrame to keep the Spark UI active
test_df = spark.createDataFrame([("Alice", 25), ("Bob", 30), ("Charlie", 35)], ["Name", "Age"])
test_df.cache()  # This keeps the DataFrame in memory so you can explore it in the UI
test_df.count()  # Force execution

print("\nA test DataFrame has been cached. You can now view it in the Spark UI at http://localhost:4040")
print("Keep this script running while you access the UI")

# Access DataFrame in SparkUI and perform some operations
test_df.show()
test_df.select("Name").show()
test_df.filter(test_df.Age > 28).show()

input("Press Enter to exit...") 