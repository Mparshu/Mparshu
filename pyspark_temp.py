from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, rand, concat, lpad
from pyspark.sql.types import StringType, LongType

def convert_columns_to_lowercase(df):
    """
    Convert all column names in a DataFrame to lowercase
    
    Args:
        df (pyspark.sql.DataFrame): Input DataFrame
    
    Returns:
        pyspark.sql.DataFrame: DataFrame with lowercase column names
    """
    # Method 1: Using select with renamed columns
    return df.select([col(c).alias(c.lower()) for c in df.columns])

def process_customer_data():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("CustomerDataProcessing") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # Step 1: Execute SQL query to create customer ID list
        customer_id_query = """
        SELECT DISTINCT customer_id 
        FROM your_source_table 
        WHERE some_filtering_conditions
        """
        
        # Execute the SQL query to get customer ID list
        customer_id_df = spark.sql(customer_id_query)
        
        # Step 2: Read Hive table
        # Replace 'your_hive_table' with actual table name
        hive_table_df = spark.table("your_hive_table")
        
        # Convert column names to lowercase
        hive_table_df = convert_columns_to_lowercase(hive_table_df)
        
        # Step 3: Filter customers based on customer ID list
        filtered_df = hive_table_df.join(
            convert_columns_to_lowercase(customer_id_df), 
            hive_table_df.customerid == customer_id_df.customerid, 
            "inner"
        ).drop(customer_id_df.customerid)
        
        # Step 4: Assign random 10-digit customer ID
        processed_df = filtered_df.withColumn("customerid", 
            concat(lpad(rand().cast("string") * 10000000000, 10, "0")).cast(StringType())
        )
        
        # Step 5: Create userdefinedscore1 column with lit(1) and cast to long
        final_df = processed_df.withColumn("userdefinedscore1", 
                                           lit(1).cast(LongType()))
        
        # Optional: Print schema to verify lowercase column names
        final_df.printSchema()
        
        # Step 6: Write to HDFS as parquet files
        final_df.write \
            .mode("append") \
            .parquet("/path/to/hdfs/output")
        
        print("Data processing completed successfully!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    
    finally:
        # Stop the Spark session
        spark.stop()

# Alternative Method 2 (if needed): 
# This method can be used if the above method doesn't work in your specific environment
def convert_columns_to_lowercase_alt(df):
    """
    Alternative method to convert column names to lowercase
    
    Args:
        df (pyspark.sql.DataFrame): Input DataFrame
    
    Returns:
        pyspark.sql.DataFrame: DataFrame with lowercase column names
    """
    # Rename columns using a dictionary comprehension
    renamed_columns = {c: c.lower() for c in df.columns}
    return df.toDF(*renamed_columns.values())

# Execute the main processing function
if __name__ == "__main__":
    process_customer_data()