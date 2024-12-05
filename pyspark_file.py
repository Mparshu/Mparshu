from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, expr

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Update CustomerID") \
    .getOrCreate()

# Read Parquet file into a DataFrame
input_path = "path/to/input/parquet"
df = spark.read.parquet(input_path)

# Drop the existing customerid column
df = df.drop("customerid")

# Add a new customerid column with serial numbers starting from 1000180
# monotonically_increasing_id() generates unique IDs, which are then offset
df = df.withColumn("customerid", expr("monotonically_increasing_id() + 1000180"))

# Save the updated DataFrame to a Parquet file
output_path = "path/to/output/parquet"
df.write.mode("overwrite").parquet(output_path)

# Stop the Spark session
spark.stop()