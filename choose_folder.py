import os
import time

# Function to get folders ending with 'csv_data'
def get_csv_data_folders(base_path):
    return [folder for folder in os.listdir(base_path) if folder.endswith('csv_data')]

# Function to display folders and ask user to choose one
def select_folder(folders):
    print("Select the folder you want to use:")
    for i, folder in enumerate(folders):
        print(f"{i + 1}. {folder}")
    
    while True:
        try:
            choice = int(input("Enter the number of your choice: "))
            if 1 <= choice <= len(folders):
                return folders[choice - 1]
            else:
                print("Invalid choice. Please select a valid number.")
        except ValueError:
            print("Invalid input. Please enter a number.")

# Main script
if __name__ == "__main__":
    base_path = "C:\\Users\\pravisha\\Downloads\\"
    folders = get_csv_data_folders(base_path)

    if not folders:
        print("No folders ending with 'csv_data' found.")
    else:
        selected_folder = select_folder(folders)
        print(f"You selected: {selected_folder}")

        # Wait for 10 seconds before exiting
        time.sleep(10)
