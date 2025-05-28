from pyspark.sql import SparkSession

# Create a Spark session without Hive support
spark = SparkSession.builder \
    .appName("Spark without Hive") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .getOrCreate()

# Create a simple DataFrame
data = [("1", "John"), ("2", "Jane")]
df = spark.createDataFrame(data, ["id", "name"])

# Show the DataFrame
print("Created DataFrame:")
df.show()

# Save as Parquet file
df.write.mode("overwrite").parquet("hdfs://namenode:9000/user/test_data.parquet")

# Read it back
print("Reading DataFrame from HDFS:")
df_read = spark.read.parquet("hdfs://namenode:9000/user/test_data.parquet")
df_read.show()

print("Checking for any data in HDFS:")
# List files in HDFS to verify data location
import subprocess
import os

# Use the hdfs command available in the container
result = subprocess.run(["hdfs", "dfs", "-ls", "/user"], 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE, 
                        text=True, 
                        check=False)
print(result.stdout)

print("Done!") 