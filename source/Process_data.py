import json


def Transform(raw_json_file):
    """
    Transforms raw movie JSON data into a cleaned format.
    - Validates schema consistency
    - Handles missing values
    - Converts complex data types (list, dict) into strings
    - Saves cleaned output to a new JSON file
    """
    try:
        # --- Load raw JSON file ---
        try:
            with open(raw_json_file, 'r') as raw_data:
                data = json.load(raw_data)
        except FileNotFoundError:
            raise FileNotFoundError(f"Cant find file{raw_json_file}")
        
        # --- Validate column consistency using first row as reference ---
        column_list = list(data[1].keys())
        for indexNo, row in enumerate(data):
            if all(key in column_list for key in list(row.keys())):
                continue
            else:
                raise ValueError(
                    f'Row number:{indexNo} has unmatching columns')
        
        # --- Data validation and transformation ---
        for indexNo, row in enumerate(data):
            for key, value in row.items():
                # Check for missing or empty values
                if value is None or len(str(value).strip()) == 0:
                    raise ValueError(
                        f"Value is missing for: '{key}' at row number: {indexNo}")
                elif isinstance(value, (list, tuple, set)):            # Convert list/tuple/set to comma-separated string
                    items = []
                    for item in value:
                        items.append(str(item))
                    row[key] = ', '.join(items)
                elif isinstance(value, dict):                           # Convert dictionary to key:value string format
                    items = []
                    for key, value in value.items():
                        items.append(f"{key}:{value}")
                        row[key] = ", ".join(items)
        
         # --- Save cleaned data to new JSON file ---
        with open('data/cleaned_top100_list.json', 'w') as file:
            json.dump(data, file, indent=4)
        print('Data loaded to the cleaned json file')
    
     # --- Error handling ---
    except FileNotFoundError as e:
        print(f"Error occured:\n{e}")
    except ValueError as e:
        print(f"Error occured:\n{e}")
    except json.JSONDecodeError as e:
        print(f"Error occured:\n{e}")
    except Exception as e:
        print(f"Error occured:\n{e}")

# --- Execute transformation ---
Transform('data/raw_data_movies_top_100.json')
