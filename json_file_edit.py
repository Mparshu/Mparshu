import json
import random

def randomize_customer_ids(input_file, output_file, customer_id_list):
    """
    Read a JSON file and assign a random customerid from the given list.
    
    Args:
    input_file (str): Path to the input JSON file
    output_file (str): Path to save the modified JSON file
    customer_id_list (list): List of customer IDs to randomly assign
    """
    try:
        # Read the input JSON file
        with open(input_file, 'r') as f:
            data = json.load(f)
        
        # Randomize customer IDs
        for item in data:
            # Randomly select a customer ID from the list
            item['customerid'] = random.choice(customer_id_list)
        
        # Write the modified data to a new JSON file
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"Successfully processed {input_file}. Output saved to {output_file}")
    
    except FileNotFoundError:
        print(f"Error: Input file {input_file} not found.")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {input_file}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Example usage
if __name__ == "__main__":
    # Sample customer ID list
    customer_ids = ['CUST001', 'CUST002', 'CUST003', 'CUST004', 'CUST005']
    
    # Specify input and output file paths
    input_file = 'input_data.json'
    output_file = 'output_data.json'
    
    # Call the function to randomize customer IDs
    randomize_customer_ids(input_file, output_file, customer_ids)






import json
import random
import os

# Path to the JSON file
file_path = "path/to/your/file.json"

# List of random pdscore values to choose from
pdscore_random_value_list = [0.5, 1.2, 3.4, 2.8, 4.1, 5.0]

# Function to generate a random 10-digit number
def generate_random_customerid():
    return str(random.randint(10**9, 10**10 - 1))

# Update the JSON file
def update_json_file(file_path):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    # Read the JSON file
    with open(file_path, 'r') as file:
        data = json.load(file)

    # Update the fields in the JSON data
    for record in data:
        record['customerid'] = generate_random_customerid()
        record['pdscore'] = random.choice(pdscore_random_value_list)

    # Save the updated JSON back to the same file
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

    print(f"File updated successfully: {file_path}")

# Execute the update function
update_json_file(file_path)