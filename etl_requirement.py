from __future__ import print_function  # For Python 3-style print in Python 2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV with JSON Column to Parquet") \
    .getOrCreate()

# Define the schema for the JSON column
# Assume you have already defined the schema as `json_schema`
# Example:
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# json_schema = StructType([
#     StructField("key1", StringType(), True),
#     StructField("key2", IntegerType(), True)
# ])

# File path to the input CSV file
csv_file_path = "/path/to/your/csv_file.csv"

# Read the CSV file
csv_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Rename columns to remove "table_name." prefix
renamed_columns = {col_name: col_name.split(".")[1] if "." in col_name else col_name for col_name in csv_df.columns}
renamed_df = csv_df.select([col(c).alias(new_c) for c, new_c in renamed_columns.items()])

# Specify the name of the column containing JSON data
json_column = "your_json_column"

# Parse the JSON column using from_json
parsed_df = renamed_df.withColumn("parsed_json", from_json(col(json_column), json_schema))

# Drop the original JSON column if no longer needed
final_df = parsed_df.drop(json_column)

# Save the DataFrame in Parquet format in the same directory as the input CSV
output_path = "/".join(csv_file_path.split("/")[:-1])  # Extract directory path from file path
parquet_output_path = "{}/output_parquet".format(output_path)  # Use Python 2-style string formatting
final_df.write.mode("overwrite").parquet(parquet_output_path)

# Stop the Spark session
spark.stop()

print("Parquet file saved at: {}".format(parquet_output_path))