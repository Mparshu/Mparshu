from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, struct, col, when, rand
from pyspark.sql.types import LongType, StructType, StructField

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Parquet Transformation") \
    .getOrCreate()

# Read parquet files
input_path = "path/to/input/parquet/"
output_path = "path/to/output/parquet/"

# Read parquet files into DataFrame
df = spark.read.parquet(input_path)

# Add exp1 and exp2 columns with lit(1) cast to LongType
df_with_exp = df \
    .withColumn("exp1", lit(1).cast(LongType())) \
    .withColumn("exp2", lit(1).cast(LongType()))

# Create random fullscore column with values 879 or 880
df_with_score = df_with_exp \
    .withColumn("fullscore", 
        when(rand() > 0.5, lit(880).cast(LongType()))
        .otherwise(lit(879).cast(LongType()))
    )

# Create struct column exp3 with customerid and fullscore
df_final = df_with_score \
    .withColumn("exp3", 
        struct(
            col("customerid").alias("customerid"),
            col("fullscore").alias("fullscore")
        )
    )

# Write the final DataFrame to parquet
df_final.write \
    .mode("overwrite") \
    .parquet(output_path)

# Stop Spark Session
spark.stop()