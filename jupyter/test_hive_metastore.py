from pyspark.sql import SparkSession

# Create a Spark session with Hive support
spark = SparkSession.builder \
    .appName("Hive Test") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Query Hive
print("Showing databases:")
result = spark.sql("SHOW DATABASES")
result.show()

# Create a test database and table
print("Creating test database and table:")
spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
spark.sql("USE test_db")

# Create a simple dataframe
data = [("1", "John", 30), ("2", "Jane", 25), ("3", "Bob", 40)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.write.mode("overwrite").saveAsTable("test_db.employees")

# Query the table
print("Querying test table:")
result = spark.sql("SELECT * FROM test_db.employees")
result.show()
