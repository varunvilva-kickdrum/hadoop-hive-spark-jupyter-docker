from pyspark.sql import SparkSession

# Create a Spark session with Hive support
spark = SparkSession.builder \
    .appName("Hive Test") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive:9083") \
    .config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres:5432/metastore") \
    .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
    .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "hive") \
    .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Query Hive
print("Showing databases:")
result = spark.sql("SHOW DATABASES")
result.show() 