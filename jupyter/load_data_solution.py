import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkFiles

# Create a properly configured Spark session
spark = SparkSession.builder \
    .appName("Dataset Access Test") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.host", "jupyter") \
    .config("spark.ui.enabled", "true") \
    .getOrCreate()

print(f"Spark version: {spark.version}")
print(f"Spark master: {spark.sparkContext.master}")

# ========================================================================
# SOLUTION 1: Using sparkContext.addFile() to distribute the file to workers
# ========================================================================
print("\n=============================================")
print("SOLUTION 1: USING SPARKCONTEXT.ADDFILE")
print("=============================================")

# Step 1: Locate the file from the driver's perspective
driver_path = "/home/jovyan/datasets/cities.csv"

if os.path.exists(driver_path):
    print(f"File exists on driver at: {driver_path}")
    
    # Step 2: Add file to Spark (will be distributed to all workers)
    spark.sparkContext.addFile(driver_path)
    
    # Step 3: Read using SparkFiles.get() to get the distributed file path
    from pyspark import SparkFiles
    distributed_path = SparkFiles.get("cities.csv")
    print(f"Distributed file path: {distributed_path}")
    
    try:
        # Read the file using the distributed path
        df = spark.read.option("header", "true") \
                  .option("inferSchema", "true") \
                  .csv(f"file://{distributed_path}")
        
        # Show results
        count = df.count()
        print(f"SUCCESS: Read {count} rows using addFile approach")
        print("Sample data:")
        df.show(5)
    except Exception as e:
        print(f"ERROR: {str(e)}")
else:
    print(f"ERROR: File not found at {driver_path}")

# ========================================================================
# SOLUTION 2: Creating a temporary view from a pandas DataFrame
# ========================================================================
print("\n=============================================")
print("SOLUTION 2: USING PANDAS DATAFRAME CONVERSION")
print("=============================================")

try:
    # Step 1: Read the file using pandas
    import pandas as pd
    pdf = pd.read_csv("/home/jovyan/datasets/cities.csv")
    print(f"Read {len(pdf)} rows with pandas")
    
    # Step 2: Convert to Spark DataFrame
    df2 = spark.createDataFrame(pdf)
    
    # Step 3: Show results
    print(f"SUCCESS: Converted to Spark DataFrame with {df2.count()} rows")
    print("Sample data:")
    df2.show(5)
except Exception as e:
    print(f"ERROR: {str(e)}")

print("\n=============================================")
print("TEST COMPLETE")
print("=============================================")
spark.stop() 