from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, rand, concat, lpad
from pyspark.sql.types import StringType, LongType

def process_customer_data():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("CustomerDataProcessing") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # Step 1: Execute SQL query to create customer ID list
        # Note: Replace with your actual SQL query
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
        
        # Step 3: Filter customers based on customer ID list
        filtered_df = hive_table_df.join(customer_id_df, 
                                         hive_table_df.customer_id == customer_id_df.customer_id, 
                                         "inner") \
                                    .drop(customer_id_df.customer_id)
        
        # Step 4: Assign random 10-digit customer ID
        processed_df = filtered_df.withColumn("customerid", 
            concat(lpad(rand().cast("string") * 10000000000, 10, "0")).cast(StringType())
        )
        
        # Step 5: Create userdefinedscore1 column with lit(1) and cast to long
        final_df = processed_df.withColumn("userdefinedscore1", 
                                           lit(1).cast(LongType()))
        
        # Step 6: Write to HDFS as parquet files
        # Replace '/path/to/hdfs/output' with your actual HDFS output path
        final_df.write \
            .mode("append") \
            .parquet("/path/to/hdfs/output")
        
        print("Data processing completed successfully!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    
    finally:
        # Stop the Spark session
        spark.stop()

# Execute the main processing function
if __name__ == "__main__":
    process_customer_data()