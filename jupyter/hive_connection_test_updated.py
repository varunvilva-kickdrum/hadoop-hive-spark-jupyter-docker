from pyspark.sql import SparkSession
import os

# Find all JARs in the hive-jars directory
hive_jars_dir = "/home/jovyan/hive-jars"
hive_jars = [os.path.join(hive_jars_dir, jar) for jar in os.listdir(hive_jars_dir) if jar.endswith('.jar')]
hive_jars_str = ",".join(hive_jars)

# Add Hudi JAR as well
hudi_jar = "/opt/spark/jars/hudi-spark3-bundle_2.12-0.14.0.jar"
all_jars = hive_jars_str + "," + hudi_jar if os.path.exists(hudi_jar) else hive_jars_str

print(f"Using JARs: {all_jars}")

# Create a Spark session with Hive support and explicit JAR files
spark = SparkSession.builder \
    .appName("Hive Test") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.jars", all_jars) \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive:10000") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive:10000") \
    .config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres:5432/metastore") \
    .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
    .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "hive") \
    .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Try creating a local table first
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