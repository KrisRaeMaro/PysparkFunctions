from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, unix_timestamp, to_date, to_timestamp, broadcast, lit,
    from_unixtime, posexplode, split, length, when, concat_ws,
    array, transform, expr
)
from pyspark.sql.types import IntegerType, LongType, ArrayType, StructType, StructField, StringType
import pandas as pd
from datetime import datetime
from pyspark.sql import Row

# Configure Spark for optimal performance
def get_optimized_spark():
    """Create and return an optimized Spark session"""
    return (SparkSession.builder
            .config("spark.sql.autoBroadcastJoinThreshold", "100MB")  # Increase broadcast join threshold
            .config("spark.sql.shuffle.partitions", "200")  # Adjust based on your cluster size
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "8g")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "2g")
            .config("spark.speculation", "true")  # Enable speculative execution
            .config("spark.sql.adaptive.enabled", "true")  # Enable adaptive query execution
            .getOrCreate())

spark = get_optimized_spark()

# Schema definition for better performance 
topsheet_schema = StructType([
    StructField("OuraID", StringType(), True),
    StructField("StudyStart", StringType(), True),
    StructField("StudyEnd", StringType(), True)
    # Add more fields as needed
])


