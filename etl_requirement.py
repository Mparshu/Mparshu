from __future__ import print_function  # For Python 3-style print in Python 2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DecimalType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV with Array of JSON Column to Parquet") \
    .getOrCreate()

# Define the schema for the JSON array
json_schema = ArrayType(StructType([
    StructField("key1", StringType(), True),
    StructField("key2", IntegerType(), True)
]))

# File path to the input CSV file
csv_file_path = "/path/to/your/csv_file.csv"

# Read the CSV file
csv_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Rename columns to remove "table_name." prefix
renamed_columns = {col_name: col_name.split(".")[1] if "." in col_name else col_name for col_name in csv_df.columns}
renamed_df = csv_df.select([col(c).alias(new_c) for c, new_c in renamed_columns.items()])

# Specify the name of the column containing the array of JSON data
json_column = "your_json_column"

# Parse the JSON column containing an array of JSON objects
parsed_df = renamed_df.withColumn("parsed_json", from_json(col(json_column), json_schema))

# Drop the original JSON column
df_no_json = parsed_df.drop(json_column)

# Check and drop "run_date" column if it exists
if "run_date" in df_no_json.columns:
    df_no_json = df_no_json.drop("run_date")

# Cast all other columns (excluding "parsed_json") to DecimalType(12, 0)
decimal_columns = [
    col(c).cast(DecimalType(12, 0)).alias(c) 
    for c in df_no_json.columns if c != "parsed_json"
]
final_df = df_no_json.select(decimal_columns + [col("parsed_json")])

# Save the DataFrame in Parquet format in the same directory as the input CSV
output_path = "/".join(csv_file_path.split("/")[:-1])  # Extract directory path from file path
parquet_output_path = "{}/output_parquet".format(output_path)  # Use Python 2-style string formatting
final_df.write.mode("overwrite").parquet(parquet_output_path)

# Stop the Spark session
spark.stop()

print("Parquet file saved at: {}".format(parquet_output_path))