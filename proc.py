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

# Load TopSheet with defined schema for better performance
def load_topsheet(path: str):
    """Load TopSheet data with defined schema for better performance"""
    df = spark.read.option("header", True).schema(topsheet_schema).csv(path)
    
    # Perform multiple transformations in a single select for better performance
    return df.select(
        col("OuraID").alias("oura_id"),
        to_date(col("StudyStart")).alias("date_start"),
        to_date(col("StudyEnd")).alias("date_end"),
        to_timestamp(col("StudyStart")).alias("utc_start"),
        to_timestamp(col("StudyEnd")).alias("utc_end")
    )

# Read CSV with error handling and caching for reused dataframes
def read_csv_or_none(path, cache=False):
    """Read CSV with robust error handling"""
    try:
        df = spark.read.option("header", True).csv(path)
        if cache:
            df = df.cache()  # Cache only if specified
        return df
    except Exception as e:
        print(f"Error reading CSV {path}: {e}")
        return None

# Filter by enrollment using broadcast join for better performance
def filter_by_enrollment(df, topsheet_df, time_col):
    """Filter records by enrollment period using broadcast join"""
    # Use broadcast hint for small dataframe
    return df.join(
        broadcast(topsheet_df), 
        "oura_id", 
        "inner"
    ).filter(
        (col(time_col) >= col("utc_start")) & 
        (col(time_col) <= col("utc_end"))
    ).drop("utc_start", "utc_end", "date_start", "date_end")

# Replace Python UDF with Spark SQL function for better performance
def get_offset_to_local(timestamp_col):
    """Get timezone offset using Spark functions instead of UDF"""
    return expr(f"cast(cast(cast({timestamp_col} as timestamp) as long) - unix_timestamp({timestamp_col}) as int) / 60")

# Optimized timeseries unpacking using Spark SQL expressions
def unpack_timeseries_sql(df, col_json, new_col, freq_sec, isnumeric):
    """Unpack timeseries data using Spark SQL for better performance"""
    try:
        # Register the DataFrame as a temp view
        df.createOrReplaceTempView("source_data")
        
        value_type = "double" if isnumeric else "int"
        
        # SQL to unpack timeseries
        unpacked_sql = f"""
        WITH parsed AS (
            SELECT 
                oura_id,
                from_json(regexp_replace({col_json}, 'null', 'NULL'), 
                         'struct<timestamp:string, items:array<{value_type}>>') as parsed_data
            FROM source_data
            WHERE {col_json} IS NOT NULL AND {col_json} != ''
        ),
        with_times AS (
            SELECT 
                oura_id,
                to_timestamp(parsed_data.timestamp) as start_time,
                parsed_data.items
            FROM parsed
            WHERE parsed_data IS NOT NULL
        ),
        exploded AS (
            SELECT 
                oura_id,
                start_time,
                pos,
                item as {new_col},
                date_format(start_time + interval pos*{freq_sec} second, 'yyyy-MM-dd HH:mm:ss') as timestamp,
                unix_timestamp(start_time) + pos*{freq_sec} as utc_timestamp,
                {get_offset_to_local('start_time + interval pos*' + str(freq_sec) + ' second')} as offset2local_min
            FROM (
                SELECT 
                    oura_id, 
                    start_time, 
                    posexplode(items) as (pos, item)
                FROM with_times
            )
        )
        SELECT * FROM exploded
        """
        
        result = spark.sql(unpacked_sql)
        
        if result.count() == 0:
            print(f"[WARN] Empty output for {col_json}")
            return None
            
        return result
    except Exception as e:
        print(f"[ERROR] unpack_timeseries_sql failed: {e}")
        return None

# Optimized flat series unpacking
def unpack_flat_series(df, start_col, series_col, out_col, freq_sec):
    """Unpack flat series data with better error handling and performance"""
    try:
        # Check for data presence and format
        print(f"Checking {series_col} format...")
        sample = df.filter(col(series_col).isNotNull() & (length(col(series_col)) > 0)).limit(5)
        
        if sample.count() == 0:
            print(f"No valid data found in {series_col}")
            return None
            
        # Register the DataFrame as a temp view for SQL processing
        df.createOrReplaceTempView("flat_series_source")
        
        # Determine delimiter based on sample
        sample_data = sample.select(series_col).collect()
        has_commas = any(',' in row[series_col] for row in sample_data if row[series_col])
        delimiter = "," if has_commas else ""
        
        # SQL for processing flat series
        unpacked_sql = f"""
        WITH split_data AS (
            SELECT 
                oura_id,
                to_timestamp({start_col}) as start_timestamp,
                split({series_col}, '{delimiter}') as values_array
            FROM flat_series_source
            WHERE {series_col} IS NOT NULL AND length({series_col}) > 0
        ),
        exploded AS (
            SELECT 
                oura_id,
                start_timestamp,
                pos,
                val,
                unix_timestamp(start_timestamp) as start_unix,
                unix_timestamp(start_timestamp) + (pos * {freq_sec}) as sample_unix
            FROM (
                SELECT 
                    oura_id, 
                    start_timestamp, 
                    posexplode(values_array) as (pos, val)
                FROM split_data
            )
        )
        SELECT 
            oura_id,
            from_unixtime(sample_unix) as timestamp,
            sample_unix as utc_timestamp,
            CASE 
                WHEN '{out_col}' IN ('hr', 'hrv', 'movement') THEN 
                    CAST(val AS INT)
                ELSE val
            END as {out_col}
        FROM exploded
        """
        
        result = spark.sql(unpacked_sql)
        print(f"Generated {result.count()} records for {out_col}")
        
        return result
    except Exception as e:
        print(f"[ERROR] unpack_flat_series failed: {e}")
        import traceback
        traceback.print_exc()
        return None

# Write results efficiently
def write_delta_partitioned(df, table_path: str, partition_col: str = "oura_id", mode="overwrite"):
    """Write data to Delta format with optimal partitioning"""
    if df is None or df.count() == 0:
        print(f"DataFrame is empty or None - skipping write for {table_path}")
        return False
        
    if partition_col not in df.columns:
        print(f"Partition column {partition_col} not found in DataFrame - writing without partition")
        df.write.mode(mode).format("delta").saveAsTable(table_path)
        return True
    
    try:
        # Repartition by partition column for better write performance
        optimal_partitions = max(200, df.rdd.getNumPartitions() // 2)  # Adjust based on data size
        
        df.repartition(optimal_partitions, col(partition_col)) \
          .write \
          .mode(mode) \
          .format("delta") \
          .partitionBy(partition_col) \
          .saveAsTable(table_path)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to write {table_path}: {e}")
        return False

# Main processing function
def clean_oura(sleep_path, activity_path, hypno_path, topsheet_path):
    """Process and clean Oura ring data with optimized operations"""
    # Load dataframes
    topsheet_df = load_topsheet(topsheet_path)
    
    # Cache topsheet for multiple uses
    topsheet_df.cache()
    
    # Load primary dataframes with caching for multiple use
    sleep_df = read_csv_or_none(sleep_path, cache=True)
    activity_df = read_csv_or_none(activity_path, cache=True)
    hypno_df = read_csv_or_none(hypno_path, cache=True)
    
    output_tables = {}
    
    # Process sleep data if available
    if sleep_df and hypno_df:
        # Find common columns for join
        common_cols = list(set(sleep_df.columns) & set(hypno_df.columns))
        print(f"Sleep count: {sleep_df.count()}")
        print(f"Hypno count: {hypno_df.count()}")
        
        # Join sleep and hypno data with broadcast for performance
        sleep_df = sleep_df.join(broadcast(hypno_df), on=common_cols, how='left')
        print(f"Joint count: {sleep_df.count()}")
        
        # Standardize column names
        sleep_df = sleep_df.withColumn("period_id", concat_ws(":", col("day"), col("period"))) \
                           .withColumnRenamed('participant_id', 'oura_id')
        
        # Extract sleep statistics
        sleep_stats_cols = [
            "oura_id", "day", "period_id", "bedtime_start", "bedtime_end",
            "average_heart_rate", "average_hrv", "average_breath",
            "awake_time", "deep_sleep_duration", "light_sleep_duration",
            "rem_sleep_duration", "latency", "lowest_heart_rate",
            "time_in_bed", "total_sleep_duration", "low_battery_alert"
        ]
        
        sleep_stats = sleep_df.select(*sleep_stats_cols)
        
        # Add offset calculation using SQL expression instead of UDF
        sleep_stats = sleep_stats.withColumn(
            "offset2local_min", 
            get_offset_to_local("bedtime_start")
        )
        
        # Convert timestamps in a single transformation
        sleep_stats = sleep_stats.select(
            "*",
            to_timestamp(col("bedtime_start")).alias("start_utc_timestamp"),
            to_timestamp(col("bedtime_end")).alias("end_utc_timestamp")
        ).drop("bedtime_start", "bedtime_end")
        
        # Filter by enrollment if needed
        # sleep_stats = filter_by_enrollment(sleep_stats, topsheet_df, "start_utc_timestamp")
        
        output_tables['sleep_stats_raw'] = sleep_stats
        
        # Process time series data
        timeseries_configs = [
            ("heart_rate", "hr", 300, True),
            ("hrv", "hrv", 300, True),
            ("movement_30_sec", "movement", 30, False),
            ("sleep_phase_5_min", "sleep_stage", 300, False),
            ("sleep_phase_30_second", "sleep_stage", 30, False),
        ]
        
        for colname, new_col, freq, isnumeric in timeseries_configs:
            print(f"\n===== PROCESSING COLUMN: {colname} =====")
            
            if colname not in sleep_df.columns:
                print(f"Column {colname} not in DataFrame - skipping")
                continue
                
            # Check for data
            data_check = sleep_df.select(
                when(col(colname).isNull(), "NULL")
                .when(length(col(colname)) == 0, "EMPTY")
                .otherwise("HAS_DATA").alias("data_status")
            ).groupBy("data_status").count()
            
            data_check.show()
            
            try:
                # Select only needed columns for processing
                subset_df = sleep_df.select('oura_id', 'day', 'period_id', 'type', 'bedtime_start', colname)
                
                # Choose appropriate unpacking method
                if colname in ['movement_30_sec', 'sleep_phase_5_min', 'sleep_phase_30_second']:
                    unpacked = unpack_flat_series(
                        subset_df,
                        start_col='bedtime_start',
                        series_col=colname,
                        out_col=new_col,
                        freq_sec=freq
                    )
                else:
                    unpacked = unpack_timeseries_sql(  # Using SQL-based unpacking
                        subset_df,
                        col_json=colname,
                        new_col=new_col,
                        freq_sec=freq,
                        isnumeric=isnumeric
                    )
                    
                if unpacked:
                    # Filter by enrollment if needed
                    # unpacked = filter_by_enrollment(unpacked, topsheet_df, "utc_timestamp")
                    
                    print(f"Successfully unpacked {colname} to {unpacked.count()} records")
                    
                    label = f"{new_col}_raw" if new_col != "sleep_stage" else f"sleep_raw_{freq}s"
                    output_tables[label] = unpacked
                    
            except Exception as e:
                print(f"Failed processing {colname}: {e}")
                import traceback
                traceback.print_exc()
    
    # Process activity data if available
    if activity_df:
        # Standardize column names and convert timestamps
        activity_df = activity_df.withColumnRenamed('participant_id', 'oura_id')
        activity_df = activity_df.withColumn(
            "start_utc_timestamp", 
            to_timestamp(col("timestamp"))
        ).drop("timestamp")
        
        # Process contributors using SQL instead of Python UDF
        activity_df.createOrReplaceTempView("activity_data")
        
        contributors_df = spark.sql("""
            SELECT 
                oura_id,
                from_json(regexp_replace(contributors, 'null', 'NULL'), 
                         'map<string,double>') as contributors_map
            FROM activity_data
            WHERE contributors IS NOT NULL
        """)
        
        # Flatten the contributors map
        contributor_cols = ["meet", "non_wear", "rest", "walk", "run"]
        
        for col_name in contributor_cols:
            contributors_df = contributors_df.withColumn(
                col_name, 
                col(f"contributors_map.{col_name}")
            )
        
        contributors_df = contributors_df.drop("contributors_map")
        
        # Join activity with contributors
        activity_cleaned = activity_df.withColumn("date", col("day"))
        activity_joined = activity_cleaned.join(contributors_df, "oura_id", "left")
        
        # Filter by enrollment if needed
        # activity_joined = filter_by_enrollment(activity_joined, topsheet_df, "start_utc_timestamp")
        
        output_tables['activity_raw'] = activity_joined
        
        # Process activity time series
        activity_timeseries = [
            ("met", "met", 60, True),
            ("class_5_min", "class_5_min", 300, True),
        ]
        
        for colname, new_col, freq, isnumeric in activity_timeseries:
            print(f"\n===== PROCESSING ACTIVITY COLUMN: {colname} =====")
            
            if colname not in activity_joined.columns:
                print(f"Column {colname} not in DataFrame - skipping")
                continue
                
            # Check for data
            col_stats = activity_joined.select(
                when(col(colname).isNull(), "NULL")
                .when(length(col(colname)) == 0, "EMPTY")
                .otherwise("HAS_DATA").alias("data_status")
            ).groupBy("data_status").count()
            
            col_stats.show()
            
            try:
                # Select only needed columns for processing
                subset_df = activity_joined.select('oura_id', 'day', colname)
                
                # Choose appropriate unpacking method
                if colname in ['class_5_min']:
                    unpacked = unpack_flat_series(
                        subset_df,
                        start_col='day',
                        series_col=colname,
                        out_col=new_col,
                        freq_sec=freq
                    )
                else:
                    unpacked = unpack_timeseries_sql(
                        subset_df,
                        col_json=colname,
                        new_col=new_col,
                        freq_sec=freq,
                        isnumeric=isnumeric
                    )
                    
                if unpacked:
                    print(f"Successfully unpacked {colname} to {unpacked.count()} records")
                    
                    label = f"{new_col}_raw_{freq}s"
                    output_tables[label] = unpacked
                    
            except Exception as e:
                print(f"Failed processing activity {colname}: {e}")
                import traceback
                traceback.print_exc()
    
    # Unpersist cached DataFrames
    if topsheet_df:
        topsheet_df.unpersist()
    if sleep_df:
        sleep_df.unpersist()
    if activity_df:
        activity_df.unpersist()
    if hypno_df:
        hypno_df.unpersist()
        
    return output_tables
