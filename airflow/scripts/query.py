#!/usr/bin/env python3
"""
Electric Vehicle Data Query Script for Airflow
Creates Hive table and runs analytics queries
"""

import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def query_ev_data():
    """
    Create Hive table from Hudi data and run analytics queries
    """
    try:
        # Create a Spark session with Hive support
        logger.info("Creating Spark session with Hive support...")
        spark = SparkSession.builder \
            .appName("ev data load to hive") \
            .config("spark.master", "spark://spark-master:7077") \
            .config("spark.jars", "/opt/spark/jars/hudi-spark3-bundle_2.12-1.0.0.jar") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .config("hive.metastore.uris", "thrift://hive:9083") \
            .config("spark.executor.cores", "2")\
            .enableHiveSupport() \
            .getOrCreate()

        # Create database if not exists
        logger.info("Creating database if not exists...")
        spark.sql("CREATE DATABASE IF NOT EXISTS ev_data")
        spark.catalog.setCurrentDatabase("ev_data")

        # Load data from Hudi
        logger.info("Loading data from Hudi...")
        df_read = spark.read.format("hudi").load("hdfs://namenode:9000/data/hudi/ev_data_cleaned")
        
        record_count = df_read.count()
        logger.info(f"Loaded {record_count} records from Hudi")

        # Save as Hive table
        logger.info("Saving data as Hive table...")
        df_read.write.mode("overwrite").saveAsTable("ev_data.ev_vehicles_cleaned")

        # Run analytics query
        logger.info("Running analytics query...")
        result_df = spark.sql("""
        SELECT make, COUNT(vehical_number) as count
        FROM  ev_data.ev_vehicles_cleaned
        WHERE state = 'WA' AND cavf_eligibility LIKE 'Clean Alternative%'
        GROUP BY make
        ORDER BY  count DESC
        LIMIT 5;
        """)
        
        # Show results and collect for logging
        result_df.show()
        results = result_df.collect()
        
        logger.info("Top 5 EV makes in WA with Clean Alternative eligibility:")
        for row in results:
            logger.info(f"  {row['make']}: {row['count']} vehicles")

        # Stop Spark session
        spark.stop()

        return {
            'status': 'success',
            'database': 'ev_data',
            'table': 'ev_vehicles_cleaned',
            'record_count': record_count,
            'query_results': [(row['make'], row['count']) for row in results]
        }

    except Exception as e:
        logger.error(f"Error during query execution: {e}")
        if 'spark' in locals():
            spark.stop()
        raise

def main():
    """
    Main function for standalone execution
    """
    try:
        result = query_ev_data()
        logger.info(f"Query execution completed successfully: {result}")
        return result
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise

if __name__ == "__main__":
    main()