import json

# Function to write data to a JSON file
def write_to_json(table_name, last_success_date, file_path='import_status.json'):
    # Data structure to be written
    data = {
        table_name: {
            "last_success_date": last_success_date
        }
    }

    try:
        # Load existing data from the file if it exists
        with open(file_path, 'r') as file:
            file_data = json.load(file)
    except FileNotFoundError:
        file_data = {}

    # Update the existing data with the new data
    file_data.update(data)

    # Write updated data back to the JSON file
    with open(file_path, 'w') as file:
        json.dump(file_data, file, indent=4)


def update_last_success_date(table_name, new_date, file_path='data.json'):
    try:
        with open(file_path, 'r') as file:
            file_data = json.load(file)
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return
    
    if table_name in file_data:
        file_data[table_name]['last_success_date'] = new_date
    else:
        print(f"Table {table_name} not found in the data.")
        return

    # Write updated data back to the JSON file
    with open(file_path, 'w') as file:
        json.dump(file_data, file, indent=4)


# Example usage
write_to_json(
    table_name="Test",
    last_success_date="2024-08-20"
)