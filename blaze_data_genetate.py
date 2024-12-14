import json
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import subprocess
import os

def read_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def edit_json_data(data):
    for item in data:
        for key in item:
            if isinstance(item[key], (int, float)):
                item[key] = random.uniform(0, 100) if isinstance(item[key], float) else random.randint(0, 100)
            elif isinstance(item[key], str):
                item[key] = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=10))
    return data

def save_json_as_parquet(spark, data, file_name):
    temp_file = f'temp_{file_name}.json'
    with open(temp_file, 'w') as file:
        json.dump(data, file)
    
    df = spark.read.json(temp_file)
    df.write.parquet(f"{file_name}.parquet")
    os.remove(temp_file)  # Clean up the temporary JSON file

def run_xyz_process(file_name):
    subprocess.run(["bash", "-c", f"./xyz_process.sh {file_name}.parquet"])

def filter_extra_scenario_ids(spark, known_scenario_ids, parquet_file):
    processed_df = spark.read.parquet(parquet_file)
    all_scenario_ids = set(processed_df.select("scenario_id").rdd.flatMap(lambda x: x).collect())
    extra_scenario_ids = all_scenario_ids - set(known_scenario_ids)
    
    if extra_scenario_ids:
        filtered_df = processed_df.filter(~col("scenario_id").isin(known_scenario_ids))
        return filtered_df, extra_scenario_ids
    return None, set()

def save_filtered_data(spark, df, backup_location):
    df.write.parquet(backup_location)

def main(scenario_ids, iterations=10):
    spark = SparkSession.builder \
        .appName("ModularScenarioIdProcessor") \
        .getOrCreate()

    for i in range(iterations):
        # Read and edit JSON
        data = read_json('input.json')
        edited_data = edit_json_data(data)

        # Save edited data as Parquet
        save_json_as_parquet(spark, edited_data, f"output_{i}")

        # Run XYZ process
        run_xyz_process(f"output_{i}")

        # Check for extra scenario IDs and filter if necessary
        filtered_df, extra_ids = filter_extra_scenario_ids(spark, scenario_ids, "xyz_output.parquet")
        
        if extra_ids:
            backup_location = f"backup_{i}.parquet"
            save_filtered_data(spark, filtered_df, backup_location)
            print(f"Iteration {i}: New scenario IDs found. Saved {len(extra_ids)} records to {backup_location}")
        else:
            print(f"Iteration {i}: No new scenario IDs found.")

    spark.stop()

# List of 40 known scenario IDs
known_scenario_ids = [f"scenario_{i}" for i in range(40)]

# Run the process
if __name__ == "__main__":
    main(known_scenario_ids)