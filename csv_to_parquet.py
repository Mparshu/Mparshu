import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from itertools import product
from functools import reduce
from pyspark.sql import DataFrame

def load_json(json_path):
    with open(json_path, 'r') as f:
        return json.load(f)

def get_hive_table_schema(spark, database_name, table_name):
    # Switch to the respective database
    spark.sql(f"USE {database_name}")
    
    # Get the schema from the Hive table
    schema = spark.table(table_name).schema
    return schema

def validate_partition_columns(df, partition_columns):
    # Check if all partition columns exist
    for column in partition_columns:
        if column not in df.columns:
            raise Exception(f"Partition column '{column}' does not exist in the CSV file.")

    # Check for null values in any of the partition columns
    for column in partition_columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            raise Exception(f"Partition column '{column}' contains NULL values. Please clean the data.")

def get_partition_combinations(df, partition_columns):
    # Get distinct values for each partition column
    distinct_values = [df.select(column).distinct().rdd.flatMap(lambda x: x).collect() for column in partition_columns]
    
    # Generate all possible combinations of the partition values
    return list(product(*distinct_values))

def filter_by_partition_combination(df: DataFrame, partition_columns: list, combination: tuple) -> DataFrame:
    """
    Apply a filter based on the combination of partition columns and their respective values.
    """
    # Construct the filter conditions by pairing partition columns and their respective values from the combination
    filter_conditions = [col(partition_columns[i]) == combination[i] for i in range(len(partition_columns))]

    # Use reduce to combine multiple conditions using AND (equivalent to applying multiple filters together)
    return df.filter(reduce(lambda a, b: a & b, filter_conditions))

def convert_csv_to_parquet(csv_path, parquet_base_path, hdfs_base_path, schema, partition_columns):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CSV to Parquet with Partitioning") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # Read CSV file with the given schema
        df = spark.read.csv(csv_path, header=True, schema=schema)
        print(f"CSV file {csv_path} read successfully with provided schema")
        
        # Validate the partition columns
        validate_partition_columns(df, partition_columns)

        # Get all combinations of partition column values
        partition_combinations = get_partition_combinations(df, partition_columns)
        
        # Write each partition to HDFS dynamically
        for combination in partition_combinations:
            # Filter the DataFrame based on the combination of partition values
            partition_df = filter_by_partition_combination(df, partition_columns, combination)

            # Build the dynamic HDFS path based on the partition columns and values
            partition_hdfs_subpath = '/'.join([f"{partition_columns[i]}={combination[i]}" for i in range(len(partition_columns))])
            partition_hdfs_path = os.path.join(hdfs_base_path, partition_hdfs_subpath)
            
            # Save the partition data as parquet
            partition_df.write.mode('overwrite').parquet(os.path.join(parquet_base_path, partition_hdfs_subpath))
            print(f"Partition written to Parquet for {partition_hdfs_subpath}.")

            # Move the Parquet file to HDFS
            os.system(f"hdfs dfs -mkdir -p {partition_hdfs_path}")
            os.system(f"hdfs dfs -put -f {parquet_base_path}/{partition_hdfs_subpath}/*.parquet {partition_hdfs_path}")
            print(f"Parquet file for {partition_hdfs_subpath} loaded into HDFS: {partition_hdfs_path}")
    
    except AnalysisException as ae:
        print(f"An error occurred with the partition column: {ae}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        spark.stop()

def process_files_from_json(json_path, parquet_base_path):
    # Initialize Spark session with Hive support enabled
    spark = SparkSession.builder \
        .appName("CSV to Parquet with Hive") \
        .enableHiveSupport() \
        .getOrCreate()

    # Load the JSON metadata
    data = load_json(json_path)

    # Loop through each file entry in the JSON
    for entry in data["files"]:
        csv_file_path = entry["csv_file_path"]
        table_name = entry["table_name"]
        database_name = entry["database_name"]
        hdfs_base_path = entry["hdfs_base_path"]
        partition_columns = entry["partition_column"].split(',')  # Handling multiple partition columns as a comma-separated string

        # Define parquet path based on the table name and database name
        parquet_path = os.path.join(parquet_base_path, database_name, table_name)

        print(f"Processing file: {csv_file_path}")
        print(f"Parquet Path: {parquet_path}")
        print(f"HDFS Path: {hdfs_base_path}")

        # Retrieve schema from Hive for the specified table
        schema = get_hive_table_schema(spark, database_name, table_name)
        print(f"Schema for {database_name}.{table_name} retrieved from Hive")

        # Convert CSV to Parquet using the schema from Hive and load to HDFS
        convert_csv_to_parquet(csv_file_path, parquet_path, hdfs_base_path, schema, partition_columns)

if __name__ == "__main__":
    # Path to the JSON file
    json_file_path = "/path/to/your/metadata.json"
    
    # Base path where the parquet files will be temporarily stored before loading into HDFS
    parquet_base_path = "/local/parquet/storage"

    # Process the files
    process_files_from_json(json_file_path, parquet_base_path)
