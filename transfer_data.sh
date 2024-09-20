#!/bin/bash

# List of Hive tables
tables=("table1" "table2" "table3" ... "table110")

# Hive database (if using a specific one)
hive_db="your_database"

# Loop through all tables
for table in "${tables[@]}"
do
    # Get HDFS location for the specific partition (date=2024-09-10)
    partition_path=$(hive -e "USE $hive_db; DESCRIBE FORMATTED $table PARTITION (date='2024-09-10');" | grep "Location:" | awk '{print $2}')

    echo "Processing table: $table"
    echo "Partition path: $partition_path"

    # Step 1: Copy from HDFS to local on Server 1
    local_dir="/path/to/local/directory/$table"
    mkdir -p $local_dir
    hdfs dfs -copyToLocal "$partition_path" "$local_dir"

    # Step 2: Transfer from Server 1 to Server 2 using scp
    scp -r "$local_dir" username@server2:/path/on/server2/local/$table

    # Step 3: Copy from local to HDFS on Server 2
    ssh username@server2 "hdfs dfs -copyFromLocal /path/on/server2/local/$table $partition_path"
done

echo "Data transfer complete!"
