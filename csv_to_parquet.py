import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

def load_json(json_path):
    with open(json_path, 'r') as f:
        return json.load(f)

def get_hive_table_schema(spark, database_name, table_name):
    # Switch to the respective database
    spark.sql(f"USE {database_name}")
    
    # Get the schema from the Hive table
    schema = spark.table(table_name).schema
    return schema

def validate_partition_column(df, partition_column):
    # Check if partition column exists
    if partition_column not in df.columns:
        raise Exception(f"Partition column '{partition_column}' does not exist in the CSV file.")

    # Check for null values in the partition column
    null_count = df.filter(col(partition_column).isNull()).count()
    if null_count > 0:
        raise Exception(f"Partition column '{partition_column}' contains NULL values. Please clean the data.")
    
def convert_csv_to_parquet(csv_path, parquet_base_path, hdfs_base_path, schema, partition_column):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CSV to Parquet with Partitioning") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # Read CSV file with the given schema
        df = spark.read.csv(csv_path, header=True, schema=schema)
        print(f"CSV file {csv_path} read successfully with provided schema")
        
        # Validate the partition column
        validate_partition_column(df, partition_column)
        
        # Get distinct partition values in the column
        partition_values = df.select(partition_column).distinct().collect()
        
        # Write each partition to HDFS dynamically
        for row in partition_values:
            partition_value = row[partition_column]
            partition_df = df.filter(col(partition_column) == partition_value)
            
            # Construct the HDFS path dynamically
            partition_hdfs_path = os.path.join(hdfs_base_path, f"{partition_column}={partition_value}")
            
            # Save the partition data as parquet
            partition_df.write.mode('overwrite').parquet(os.path.join(parquet_base_path, f"{partition_column}={partition_value}"))
            print(f"Partition {partition_column}={partition_value} written to Parquet.")

            # Move the Parquet file to HDFS
            os.system(f"hdfs dfs -mkdir -p {partition_hdfs_path}")
            os.system(f"hdfs dfs -put -f {parquet_base_path}/{partition_column}={partition_value}/*.parquet {partition_hdfs_path}")
            print(f"Parquet file for {partition_column}={partition_value} loaded into HDFS: {partition_hdfs_path}")
    
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
        partition_column = entry["partition_column"]

        # Define parquet path based on the table name and database name
        parquet_path = os.path.join(parquet_base_path, database_name, table_name)

        print(f"Processing file: {csv_file_path}")
        print(f"Parquet Path: {parquet_path}")
        print(f"HDFS Path: {hdfs_base_path}")

        # Retrieve schema from Hive for the specified table
        schema = get_hive_table_schema(spark, database_name, table_name)
        print(f"Schema for {database_name}.{table_name} retrieved from Hive")

        # Convert CSV to Parquet using the schema from Hive and load to HDFS
        convert_csv_to_parquet(csv_file_path, parquet_path, hdfs_base_path, schema, partition_column)

if __name__ == "__main__":
    # Path to the JSON file
    json_file_path = "/path/to/your/metadata.json"
    
    # Base path where the parquet files will be temporarily stored before loading into HDFS
    parquet_base_path = "/local/parquet/storage"

    # Process the files
    process_files_from_json(json_file_path, parquet_base_path)
