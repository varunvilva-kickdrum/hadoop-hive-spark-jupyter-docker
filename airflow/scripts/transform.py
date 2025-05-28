#!/usr/bin/env python3
"""
Electric Vehicle Data Transformation Script for Airflow
Transforms raw CSV data and saves in Hudi format
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_ev_data():
    """
    Transform Electric Vehicle data from HDFS CSV to Hudi format
    """
    try:
        # Create Spark session
        logger.info("Creating Spark session...")
        spark = SparkSession.builder \
            .appName("EV Analytics Pipeline") \
            .config("spark.jars", "/opt/spark/jars/hudi-spark3-bundle_2.12-1.0.0.jar") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .master("spark://spark-master:7077")\
            .config("spark.executor.cores", "2")\
            .getOrCreate()

        # Load data from HDFS
        logger.info("Loading data from HDFS...")
        df = spark.read.load("hdfs://namenode:9000/data/Electric_Vehicle_Population_Data.csv", 
                           format="csv", sep=",", inferSchema="true", header="true")
        
        initial_count = df.count()
        logger.info(f"Loaded {initial_count} records from HDFS")

        # Rename columns as per general convention lower case and snake case
        logger.info("Renaming columns...")
        rename_cols = {
            'VIN (1-10)': 'vehical_number',
            'Postal Code': 'postal_code',
            'Electric Vehicle Type': 'vehical_type',
            'Clean Alternative Fuel Vehicle (CAFV) Eligibility': 'cavf_eligibility',
            'Electric Range': 'electric_range',
            'Base MSRP': 'base_msrp',
            'Legislative District': 'legislative_district',
            'Vehicle Location': 'vehicle_location',
            'Electric Utility': 'electric_utility',
            '2020 Cencus Tract': 'cencus_tract',
            'Model Year':'model_year'
        }

        for old_name, new_name in rename_cols.items():
            df = df.withColumnRenamed(old_name, new_name)

        # Make all columns lower case
        for col in df.columns:
            df = df.withColumnRenamed(col, col.lower())

        # Fill null values
        logger.info("Filling null values...")
        df_filled = df.fillna({
            'city':'Unknown',
            'county':'Unknown',
            'postal_code': 0,
            'electric_range': 0,
            'electric_utility': 'Unknown',
            'model_year': 0,
            'vehicle_location': 'Unknown',
            'legislative_district':0,
        })

        # Dropping unnecessary columns
        logger.info("Dropping unnecessary columns...")
        new_df = df_filled.drop('2020 census tract', 'dol vehicle id')

        # Extract lat and lon columns from vehicle_location
        logger.info("Extracting latitude and longitude...")
        new_df = new_df.withColumn('longitude', regexp_extract(col("vehicle_location"), r'POINT \((-?\d+\.\d+)', 1).cast(DoubleType()))
        new_df = new_df.withColumn("latitude", regexp_extract(col("vehicle_location"), r'POINT \(-?\d+\.\d+ (-?\d+\.\d+)\)', 1).cast(DoubleType()))

        new_df = new_df.fillna({
            'latitude':0,
            'longitude':0,
            'base_msrp':0
        })

        # Add event timestamp
        new_df = new_df.withColumn('event_time', current_timestamp())

        # Define Hudi write options
        logger.info("Preparing Hudi write options...")
        hudi_options = {
            'hoodie.table.name': 'ev_data_cleaned',
            'hoodie.datasource.write.storage.type': 'COPY_ON_WRITE',
            'hoodie.datasource.write.recordkey.field': 'vehical_number',
            'hoodie.datasource.write.partitionpath.field': 'state',
            'hoodie.datasource.write.precombine.field': 'event_time',
            'hoodie.upsert.shuffle.parallelism': '2',
            'hoodie.insert.shuffle.parallelism': '2',
            'hoodie.datasource.hive_sync.enable': 'false',
            'hoodie.datasource.write.hive_style_partitioning': 'true',
        }

        output_path = "hdfs://namenode:9000/data/hudi/ev_data_cleaned"

        # Write to Hudi
        logger.info(f"Writing transformed data to Hudi at {output_path}...")
        new_df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(output_path)

        final_count = new_df.count()
        logger.info(f"Successfully transformed and saved {final_count} records to Hudi")

        # Stop Spark session
        spark.stop()

        return {
            'status': 'success',
            'input_records': initial_count,
            'output_records': final_count,
            'output_path': output_path
        }

    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        if 'spark' in locals():
            spark.stop()
        raise

def main():
    """
    Main function for standalone execution
    """
    try:
        result = transform_ev_data()
        logger.info(f"Transformation completed successfully: {result}")
        return result
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

if __name__ == "__main__":
    main()