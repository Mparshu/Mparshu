import json
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import subprocess

def process_scenario_data(scenario_ids, iterations=10):
    spark = SparkSession.builder \
        .appName("ScenarioIdProcessor") \
        .getOrCreate()

    for i in range(iterations):
        # Step 1 & 2: Edit JSON and save it as Parquet
        with open('input.json', 'r') as file:
            data = json.load(file)
        
        for item in data:
            for key in item:
                if isinstance(item[key], (int, float)):
                    item[key] = random.uniform(0, 100) if isinstance(item[key], float) else random.randint(0, 100)
                elif isinstance(item[key], str):
                    item[key] = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=10))

        temp_json_file = f'temp_{i}.json'
        with open(temp_json_file, 'w') as file:
            json.dump(data, file)

        df = spark.read.json(temp_json_file)
        df.write.parquet(f"output_{i}.parquet")

        # Step 3: Run the xyz process
        subprocess.run(["bash", "-c", "./xyz_process.sh output_{}.parquet".format(i)])

        # Step 4: Read the output from the xyz process (assuming it's saved in 'xyz_output.parquet')
        processed_df = spark.read.parquet("xyz_output.parquet")

        # Step 5: Check for extra scenario IDs
        existing_scenario_ids = set(scenario_ids)
        all_scenario_ids = set(processed_df.select("scenario_id").rdd.flatMap(lambda x: x).collect())
        
        extra_scenario_ids = all_scenario_ids - existing_scenario_ids

        if extra_scenario_ids:
            # Step 6: Filter and save data for extra scenario IDs
            filtered_df = processed_df.filter(~col("scenario_id").isin(list(existing_scenario_ids)))
            backup_location = f"backup_{i}.parquet"
            filtered_df.write.parquet(backup_location)
            print(f"Iteration {i}: New scenario IDs found. Saved {len(extra_scenario_ids)} records to {backup_location}")
        else:
            print(f"Iteration {i}: No new scenario IDs found.")

    spark.stop()

# List of 40 known scenario IDs
known_scenario_ids = [f"scenario_{i}" for i in range(40)]

# Run the process
process_scenario_data(known_scenario_ids)