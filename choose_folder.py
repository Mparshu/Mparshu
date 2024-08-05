from pyspark.sql import SparkSession
import os
import time
import argparse

def get_csv_data_folders(base_path):
    return [folder for folder in os.listdir(base_path) if folder.endswith('csv_data')]

def select_folder(folders, partial_name):
    matching_folders = [folder for folder in folders if folder.startswith(partial_name)]
    if len(matching_folders) == 1:
        return matching_folders[0]
    elif len(matching_folders) > 1:
        raise ValueError(f"Multiple folders match the name '{partial_name}'. Please be more specific.")
    else:
        raise ValueError(f"No folder matches the name '{partial_name}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Select a csv_data folder")
    parser.add_argument("--list", action="store_true", help="List available csv_data folders")
    parser.add_argument("--select", type=str, help="Select a folder by its partial name")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("FolderSelector").getOrCreate()
    
    base_path = "/home"
    folders = get_csv_data_folders(base_path)

    if args.list:
        print("Available folders ending with 'csv_data':")
        for folder in folders:
            print(f"- {folder}")
    elif args.select:
        try:
            selected_folder = select_folder(folders, args.select)
            print(f"You selected: {selected_folder}")

            # Wait for 10 seconds before exiting
            time.sleep(10)
        except ValueError as e:
            print(e)
    else:
        parser.print_help()

    spark.stop()
