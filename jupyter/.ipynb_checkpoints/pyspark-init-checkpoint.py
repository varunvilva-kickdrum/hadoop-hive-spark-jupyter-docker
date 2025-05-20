import os
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Create a Spark session
def get_spark(app_name="JupyterPySparkApp"):
    """
    Create a properly configured Spark session
    
    Parameters:
    -----------
    app_name : str
        The name of the Spark application
        
    Returns:
    --------
    SparkSession
    """
    spark = (SparkSession
        .builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g") 
        .config("spark.cores.max", "2")
        .config("spark.executor.cores", "2")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", "jupyter")
        .config("spark.driver.port", "4040")
        .config("spark.blockManager.port", "4041")
        .getOrCreate()
    )
    
    # Print session info
    print(f"Spark version: {spark.version}")
    print(f"Connected to: {spark.sparkContext.master}")
    print(f"UI available at: http://localhost:4040")
    
    return spark

# Default spark session
spark = get_spark() 