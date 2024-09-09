import os
import json
import zipfile
import argparse
import shutil
import logging
import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

# Setup logging
def setup_logger():
    log_file = os.path.join(os.path.expanduser("~"), "Inbound", f"processing{current_timestamp}.log")
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger("DataProcessing")

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
    # Check if the partition column exists
    if partition_column not in df.columns:
        raise Exception(f"Partition column '{partition_column}' does not exist in the file.")

    # Check for null values in the partition column
    null_count = df.filter(col(partition_column).isNull()).count()
    if null_count > 0:
        raise Exception(f"Partition column '{partition_column}' contains NULL values. Please clean the data.")

def get_partition_values(df, partition_column):
    # Get distinct partition values in the column
    return df.select(partition_column).distinct().collect()

'''
def check_all_null_columns(df):
    """
    Check if all columns in the DataFrame have only NULL values.
    Returns True if all columns are NULL, False otherwise.
    """
    try:
        null_columns = df.select([spark_sum(df[col_name].isNull().cast("int")).alias(col_name) for col_name in df.columns])
        total_rows = df.count()

        # If the sum of nulls for all columns equals the total number of rows, we have an issue
        all_null = all(null_columns.collect()[0][col_name] == total_rows for col_name in df.columns)

        return all_null
    except Exception as e:
        logger.error(f"Error while checking for all NULL columns: {e}")
        raise e
'''

def check_all_null_columns(df):
    """
    Check if all columns in the DataFrame have only NULL values.
    Returns True if all columns are NULL, False otherwise.
    """
    try:
        # Calculate the sum of nulls for each column
        null_columns = df.select([sum(df[col_name].isNull().cast("int")).alias(col_name) for col_name in df.columns])

        # Count the total number of rows in the DataFrame
        total_rows = df.count()

        # Collect the results into a list
        null_counts = null_columns.collect()[0]

        # Check if the sum of nulls for all columns equals the total number of rows
        all_null = all(null_counts[col_name] == total_rows for col_name in df.columns)

        return all_null
    except Exception as e:
        logger.error(f"Error while checking for all NULL columns: {e}")
        raise e    

def read_file(spark, file_path, schema=None):
    """
    Reads a file based on its extension and returns a DataFrame.
    - If the file is CSV, it uses spark.read.csv
    - If the file is JSON, it uses spark.read.json
    """
    file_extension = os.path.splitext(file_path)[1]

    if file_extension == '.csv':
        df = spark.read.csv(file_path, header=True, schema=schema)
        print(f"CSV file {file_path} read successfully.")
    elif file_extension == '.json':
        df = spark.read.json(file_path, schema=schema)
        print(f"JSON file {file_path} read successfully.")
    else:
        raise Exception(f"Unsupported file format: {file_extension}. Only CSV and JSON files are supported.")
    
    return df

def convert_file_to_parquet(spark, file_path, hdfs_base_path, schema, partition_column):
    try:
        logger.info(f"Processing file: {file_path}")

        # Read the file based on its extension (CSV or JSON)
        df = read_file(spark, file_path, schema=schema)
        
        # Validate the partition column
        validate_partition_column(df, partition_column)

        if check_all_null_columns(df):
            logger.error(f"File structure issue detected in {file_path}. All columns have NULL values.")
            raise Exception(f"File structure issue detected in {file_path}. All columns have NULL values.")

        # Get distinct partition values for the partition column
        partition_values = get_partition_values(df, partition_column)
        
        logger.info(f"Found {len(partition_values)} partitions for column '{partition_column}' in file {file_path}")

        # Write each partition directly to HDFS
        for row in partition_values:
            partition_value = row[partition_column]
            
            # Filter the DataFrame based on the partition value
            partition_df = df.filter(col(partition_column) == partition_value)
            partition_count = partition_df.count()

            # Build the dynamic HDFS path based on the partition column and value
            partition_hdfs_path = os.path.join(hdfs_base_path, f"{partition_column}={partition_value}")
            
            logger.info(f"Loading partition '{partition_column}={partition_value}' with {partition_count} records into {partition_hdfs_path}")

            # Save the partition data as parquet directly to HDFS
            partition_df.write.mode('overwrite').parquet(partition_hdfs_path)
            logger.info(f"Partition '{partition_column}={partition_value}' loaded successfully to {partition_hdfs_path}")
            print(f"Partition written to Parquet for {partition_column}={partition_value} in HDFS: {partition_hdfs_path}")

    except AnalysisException as ae:
        logger.error(f"An error occurred with the partition column: {ae}")
        print(f"An error occurred with the partition column: {ae}")
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        print(f"An error occurred: {e}")

def unzip_file(zip_path, extract_to):
    """
    Unzips the given file to the specified location.
    """
    try:
        logger.info(f"Extracting zip file: {zip_path} to {extract_to}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            print(f"Unzipped {zip_path} to {extract_to}")

        extracted_files = os.listdir(extract_to)
        logger.info(f"Zip file extracted. Found {len(extracted_files)} files.")
    except Exception as e:
        logger.error(f"Error unzipping file {zip_path}: {e}")
        raise e

def update_file_paths(metadata, inbound_dir):
    """
    Updates the file paths in the metadata JSON to include the full inbound directory path.
    """
    for entry in metadata["files"]:
        # Prepend the inbound_dir to the file path
        entry["csv_file_path"] = os.path.join(inbound_dir, entry["csv_file_path"])
        print(f"Updated file path: {entry['csv_file_path']}")

def execute_hive_queries(spark, hql_file):
    """
    Executes the Hive queries in the provided .hql file.
    Stops execution if any query fails.
    """
    try:
        with open(hql_file, 'r') as f:
            queries = f.read().split(';')
        
        for query in queries:
            query = query.strip()
            if query:
                logger.info(f"Executing Hive query from {hql_file}: {query}")
                print(f"Executing Hive query from {hql_file}: {query}")
                query = query.rstrip(';')
                spark.sql(query)
                logger.info(f"Hive query executed successfully: {query}")
                print(f"Query executed successfully: {query}")
    
    except Exception as e:
        logger.error(f"Failed to execute Hive query in {hql_file}: {e}")
        raise Exception(f"Failed to execute Hive query in {hql_file}: {e}")

def find_and_execute_hql_files(inbound_dir, spark):
    """
    Finds and executes all .hql files in the unzipped folder sequentially.
    Stops if any query execution fails.
    """
    logger.info(f"Checking hql files from: {inbound_dir}")
    hql_files = [f for f in os.listdir(inbound_dir) if f.endswith('.hql')]
    
    if not hql_files:
        print("No .hql files found to execute.")
        return
    
    for hql_file in hql_files:
        hql_file_path = os.path.join(inbound_dir, hql_file)
        logger.info(f"Found .hql file: {hql_file_path}")
        print(f"Found .hql file: {hql_file_path}")
        
        # Execute the Hive queries from the .hql file
        execute_hive_queries(spark, hql_file_path)

def cleanup_unzipped_folder(unzipped_folder):
    """
    Deletes only the specific unzipped folder after processing is complete.
    """
    try:
        if os.path.exists(unzipped_folder):
            shutil.rmtree(unzipped_folder)
            print(f"Deleted unzipped folder: {unzipped_folder}")
        else:
            print(f"Unzipped folder {unzipped_folder} does not exist.")
    except Exception as e:
        print(f"Error occurred during cleanup: {e}")


def process_files_from_json(spark, json_path):
    try:
        # Load the JSON metadata
        data = load_json(json_path)
        files_processed = 0

        # Loop through each file entry in the JSON
        for entry in data["files"]:
            file_path = entry["csv_file_path"]
            table_name = entry["table_name"]
            database_name = entry["database_name"]
            hdfs_base_path = entry["hdfs_base_path"]
            partition_column = entry["partition_column"]

            print(f"Processing file: {file_path}")
            print(f"HDFS Path: {hdfs_base_path}")

            # Retrieve schema from Hive for the specified table
            schema = get_hive_table_schema(spark, database_name, table_name)
            print(f"Schema for {database_name}.{table_name} retrieved from Hive")

            # Convert the file (CSV or JSON) to Parquet using the schema from Hive and load directly to HDFS
            logger.info(f"Starting processing for table: {table_name} with file: {file_path}")
            convert_file_to_parquet(spark, file_path, hdfs_base_path, schema, partition_column)
            files_processed += 1

        logger.info(f"Processing complete. Total files processed: {files_processed}")
    except Exception as e:
        logger.error(f"Error in processing files from metadata: {e}")
        raise e

if __name__ == "__main__":
    # Argument parser for dynamic file selection
    parser = argparse.ArgumentParser(description="Process CSV/JSON files from ZIP and convert them to Parquet for HDFS.")
    parser.add_argument("--select", required=False, help="Path to the metadata JSON file.")
    parser.add_argument("--zip", required=False, help="Path to the ZIP file containing metadata and files.")
    
    args = parser.parse_args()

    # Initialize the logger
    logger = setup_logger()

    # Initialize Spark session with Hive support enabled
    spark = SparkSession.builder \
        .appName("File to Parquet with Hive") \
        .enableHiveSupport() \
        .getOrCreate()

    # If a ZIP file is provided, unzip it to $HOME/Inbound
    if args.zip:
        home_dir = os.path.expanduser("~")
        inbound_dir = os.path.join(home_dir, "Inbound")
        os.makedirs(inbound_dir, exist_ok=True)
        unzip_file(args.zip, inbound_dir)
        unzipped_folder_path = os.path.join(inbound_dir, os.path.splitext(os.path.basename(args.zip))[0])

        # Get the name of the unzipped folder
        unzipped_folder_name = os.path.basename(unzipped_folder_path)

        # Set the path to metadata.json after unzipping
        json_file_path = os.path.join(inbound_dir, "metadata.json")

        # Execute any .hql files found in the unzipped folder before loading data
        find_and_execute_hql_files(inbound_dir, spark)

        # Load and update metadata file paths with the full inbound path
        metadata = load_json(json_file_path)
        update_file_paths(metadata, inbound_dir)

        # Save the updated metadata to a temporary JSON file
        temp_json_path = os.path.join(inbound_dir, "updated_metadata.json")
        with open(temp_json_path, 'w') as temp_file:
            json.dump(metadata, temp_file, indent=4)
        
        # Now process the files from the updated metadata
        process_files_from_json(spark, temp_json_path)

        # Clean up only the unzipped folder after processing
        cleanup_unzipped_folder(inbound_dir)

    else:
        # Use the --select argument if no ZIP file is provided
        json_file_path = args.select
        if json_file_path:
            process_files_from_json(spark, json_file_path)
        else:
            print("Error: Please provide a JSON file using --select or a ZIP file using --zip.")

    # Stop the Spark session
    spark.stop()
