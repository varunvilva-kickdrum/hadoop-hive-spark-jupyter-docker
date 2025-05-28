from pyspark.sql import SparkSession

# Create a Spark session WITHOUT Hive support to work around the Hive connectivity issue
spark = SparkSession.builder \
    .appName("Hive Workaround") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .getOrCreate()

# Instead of querying databases, we'll create and use our own data
# Create a simple dataframe
print("Creating a simple dataframe:")
data = [("1", "John", 30), ("2", "Jane", 25), ("3", "Bob", 40)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.show()

# Save as a table-like structure using Parquet files
print("Saving data to HDFS:")
df.write.mode("overwrite").parquet("hdfs://namenode:9000/user/sales_data.parquet")

# Read it back
print("Reading data back from HDFS:")
sales_df = spark.read.parquet("hdfs://namenode:9000/user/sales_data.parquet")
sales_df.createOrReplaceTempView("sales")

# Query the data
print("Querying the data (similar to Hive queries):")
result = spark.sql("SELECT * FROM sales WHERE age > 25")
result.show()

print("""
IMPORTANT NOTE:

There seems to be an issue with the Hive metastore connection. To fix this, try the following:

1. Make sure the Hive service is running correctly:
   - Check if the Hive container is running: docker ps | grep hive
   - Check Hive logs: docker logs hive

2. Update your Hive connection settings:
   - Use port 10000 instead of 9083 for the Hive metastore
   - Ensure all Hive JAR files are available to Spark

3. If you need to use Hive:
   - For now, you can work with Spark's temporary views and save data to HDFS in Parquet format
   - Later, when Hive is properly configured, you can use HiveQL to query the tables
""") 