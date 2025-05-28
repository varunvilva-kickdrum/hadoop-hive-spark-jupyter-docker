from pyspark.sql import SparkSession

# Create a Spark session with local Hive metastore
spark = SparkSession.builder \
    .appName("Local Hive Test") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("spark.hadoop.hive.metastore.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=/home/jovyan/work/metastore_db;create=true") \
    .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver") \
    .enableHiveSupport() \
    .getOrCreate()

# Try creating a local table
print("Creating database and table...")
spark.sql("CREATE DATABASE IF NOT EXISTS local_test")
spark.sql("USE local_test")

data = [("1", "John"), ("2", "Jane")]
df = spark.createDataFrame(data, ["id", "name"])
df.write.mode("overwrite").saveAsTable("local_test.users")

# Query Hive databases
print("Showing databases:")
result = spark.sql("SHOW DATABASES")
result.show()

# Show the table we created
print("Showing tables in local_test:")
result = spark.sql("SHOW TABLES IN local_test")
result.show()

# Query the data
print("Querying data from users table:")
result = spark.sql("SELECT * FROM local_test.users")
result.show()

print("Done!") 