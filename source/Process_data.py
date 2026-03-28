import json
import pandas as pd


"""Transforming raw data from top 100 movies list"""
def Transform_top100_movie_list(raw_top100_movies_json_file):
    """
    Transforms raw movie JSON data into a cleaned format.
    - Validates schema consistency
    - Handles missing values
    - Converts complex data types (list, dict) into strings
    - Saves cleaned output to a new JSON file 
    - Return a dataframe of the table
    """
    try:
        # --- Load raw JSON file ---
        try:
            with open(raw_top100_movies_json_file, 'r') as raw_data:
                data = json.load(raw_data)
        except FileNotFoundError:
            raise FileNotFoundError(f"Cant find file{raw_top100_movies_json_file}")

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
                # Convert list/tuple/set to comma-separated string
                elif isinstance(value, (list, tuple, set)):
                    items = []
                    for item in value:
                        items.append(str(item))
                    row[key] = ', '.join(items)
                # Convert dictionary to key:value string format
                elif isinstance(value, dict):
                    items = []
                    for key, value in value.items():
                        items.append(f"{key}:{value}")
                        row[key] = ", ".join(items)

         # --- Save cleaned data to new JSON file ---
        try: 
            with open(r'data/cleaned_top100_movies_list.json', 'w') as file:
                json.dump(data, file, indent=4)
        except ValueError:
            raise ValueError(f'Finding problems with writing to:{r'data/cleaned_top100_movies_list.json'}')
         # Convert to pandas DataFrame for further analysis
        df = pd.DataFrame(data)
        return df
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
# Example usage
# try:
#     df = Transform_top100_movie_list('data/raw_data_movies_top_100.json')
#     print(df.head())
# except Exception as e:
#     print(f'Error occured:\n{e}')

"""Transforming raw data from movies data by movie id list"""
def Transform_moviesdata_by_id(raw_moviesdata_by_id_json_file):
    """
    Transform raw movie data by ID into a clean, flat structure.

    Steps:
    1. Load raw data and top100 reference list from JSON files.
    2. Identify common keys between datasets and remove duplicates (except 'id').
    3. Flatten nested structures (lists, sets, tuples, dicts) into strings.
    4. Verify all data is flattened.
    5. Save cleaned data to a new JSON file.
    6. Convert cleaned data to a pandas DataFrame and return it.
    
    Args:
        raw_moviesdata_by_id_json_file (str): Path to raw JSON file.

    Returns:
        pd.DataFrame: Cleaned and flattened data.
    """
    try:
        # Load raw movie data
        try:
            with open(raw_moviesdata_by_id_json_file, 'r') as file:
                data_by_id = json.load(file)
        except FileNotFoundError:
            raise FileNotFoundError(
                f'File:{raw_moviesdata_by_id_json_file}\n Not found')
        
        # Load top100 reference data
        try:
            with open(r'data/cleaned_top100_movies_list.json', 'r') as top100list:
                top100_data = json.load(top100list)
        except FileNotFoundError:
            raise FileNotFoundError(
                f'File:{r'data/cleaned_top100_movies_list.json'}\n Not found')
        
        # Identify keys in both datasets
        keys_list = list(data_by_id[0].keys())          #Have to view your json file first to confirm data is inside a list
        top100_key_list = list(top100_data[0].keys())
        common_keys = list(set(keys_list).intersection(set(top100_key_list)))
        common_keys.remove('id')   # Preserve unique identifier

        # Remove duplicate keys from raw data that exist in top100 (except 'id')
        for i, data in enumerate(data_by_id,):
            if all(key in list(data.keys()) for key in keys_list):
                for key in common_keys:
                    if key in data:
                        data.pop(key,None)  # Remove key if exists
                    else:
                        continue
            else:
                raise ValueError(f'Dict on index:{i} has different keys')
            
        # Flatten nested structures
        for data in data_by_id:
            for key, value in data.items():
                if isinstance(value, (list, set, tuple)):
                    # Join list/set/tuple values into a comma-separated string
                    data[key] = ", ".join(list(str(x)for x in value))

                elif isinstance(value, dict):
                     # Convert dictionary into string of key,value pairs
                    zipped_list = list(zip(value.keys(), value.values()))
                    data[key] = ','.join(
                        f"'{','.join(str(x) for x in t)}'" for t in zipped_list)
                    
                else:
                    continue    # Leave primitive types as-is

        # Verify flattening was successful
        if not all(any(isinstance(value, (dict, list, tuple)) for value in data.values()) for data in data_by_id):
            pass
        else:
            raise ValueError('The data still has nested values')
        
         # Save cleaned data to JSON
        try:
            with open(r'data/clean_moviedata_byID.json', 'w') as newfile:
                json.dump(data_by_id, newfile, indent=4)
        except ValueError:
            raise ValueError(f'Finding problems with writing to:{r'data/clean_moviedata_byID.json'}')
        
        # Convert to pandas DataFrame for further analysis
        df = pd.DataFrame(data_by_id)
        return df
    except FileNotFoundError as e:
        print(f"Error occured:\n{e}")
    except ValueError as e:
        print(f"Error occured:\n{e}")
    except json.JSONDecodeError as e:
        print(f"Error occured:\n{e}")
    except Exception as e:
        print(f"Error occured:\n{e}")
# --- Execute transformation ---
# Example usage
# try:
#     df = Transform_moviesdata_by_id(r'data/raw_moviedata_byID.json')
#     print(df.head())
# except Exception as e:
#     print(f'Error occured:\n{e}')

"""Transforming raw data from top 100 series list"""
def Transform_top100_series_list(raw_top_100_series_json_file):
    """
    Transforms raw series JSON data into a cleaned format.
    - Validates schema consistency
    - Handles missing values
    - Converts complex data types (list, dict) into strings
    - Saves cleaned output to a new JSON file 
    - Return a dataframe of the table
    """
    try:
        # --- Load raw JSON file ---
        try:
            with open(raw_top_100_series_json_file, 'r') as raw_data:
                data = json.load(raw_data)
        except FileNotFoundError:
            raise FileNotFoundError(f"Cant find file{raw_top_100_series_json_file}")

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
                # Convert list/tuple/set to comma-separated string
                elif isinstance(value, (list, tuple, set)):
                    items = []
                    for item in value:
                        items.append(str(item))
                    row[key] = ', '.join(items)
                # Convert dictionary to key:value string format
                elif isinstance(value, dict):
                    items = []
                    for key, value in value.items():
                        items.append(f"{key}:{value}")
                        row[key] = ", ".join(items)

         # --- Save cleaned data to new JSON file ---
        try: 
            with open(r'data/cleaned_top100_series_list.json', 'w') as file:
                json.dump(data, file, indent=4)
        except ValueError:
            raise ValueError(f'Finding problems with writing to:{r'data/cleaned_top100_series_list.json'}')
         # Convert to pandas DataFrame for further analysis
        df = pd.DataFrame(data)
        return df
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
# Example usage
# try:
#     df = Transform_top100_series_list(r"data/raw_data_series_top_100.json")
#     print(df.head())
#     print(df.shape)
# except Exception as e:
#     print(f'Error occured:\n{e}')

"""Transforming raw data from series data by series id list"""
def Transform_seriesdata_by_id(raw_seriesdata_by_id_json_file):
    """
    Transform raw series data by ID into a clean, flat structure.

    Steps:
    1. Load raw series data and top100 reference list from JSON files.
    2. Identify common keys between datasets and remove duplicates (except 'id').
    3. Flatten nested structures (lists, sets, tuples, dicts) into strings.
    4. Verify all data is flattened.
    5. Save cleaned data to a new JSON file.
    6. Convert cleaned data to a pandas DataFrame and return it.
    
    Args:
        raw_seriesdata_by_id_json_file (str): Path to raw JSON file.

    Returns:
        pd.DataFrame: Cleaned and flattened data.
    """
    try:
        # Load raw movie data
        try:
            with open(raw_seriesdata_by_id_json_file, 'r') as file:
                data_by_id = json.load(file)
        except FileNotFoundError:
            raise FileNotFoundError(
                f'File:{raw_seriesdata_by_id_json_file}\n Not found')
        
        # Load top100 reference data
        try:
            with open(r'data/cleaned_top100_series_list.json', 'r') as top100list:
                top100_data = json.load(top100list)
        except FileNotFoundError:
            raise FileNotFoundError(
                f'File:{r'data/cleaned_top100_series_list.json'}\n Not found')
        
        # Identify keys in both datasets
        keys_list = list(data_by_id[0].keys())      #Have to view your json file first to confirm data is inside a list
        top100_key_list = list(top100_data[0].keys())
        common_keys = list(set(keys_list).intersection(set(top100_key_list)))
        common_keys.remove('id')   # Preserve unique identifier

        # Remove duplicate keys from raw data that exist in top100 (except 'id')
        for i, data in enumerate(data_by_id,):
            if all(key in list(data.keys()) for key in keys_list):
                for key in common_keys:
                    if key in data:
                        data.pop(key,None)  # Remove key if exists
                    else:
                        continue
            else:
                raise ValueError(f'Dict on index:{i} has different keys')
            
        # Flatten nested structures
        for data in data_by_id:
            for key, value in data.items():
                if isinstance(value, (list, set, tuple)):
                    # Join list/set/tuple values into a comma-separated string
                    data[key] = ", ".join(list(str(x)for x in value))

                elif isinstance(value, dict):
                     # Convert dictionary into string of key,value pairs
                    zipped_list = list(zip(value.keys(), value.values()))
                    data[key] = ','.join(
                        f"'{','.join(str(x) for x in t)}'" for t in zipped_list)
                    
                else:
                    continue    # Leave primitive types as-is

        # Verify flattening was successful
        if not all(any(isinstance(value, (dict, list, tuple)) for value in data.values()) for data in data_by_id):
            pass
        else:
            raise ValueError('The data still has nested values')
        
         # Save cleaned data to JSON
        try:
            with open(r'data/clean_seriesdata_byID.json', 'w') as newfile:
                json.dump(data_by_id, newfile, indent=4)
        except ValueError:
            raise ValueError(f'Finding problems with writing to:{r'data/clean_seriesdata_byID.json'}')
        
        # Convert to pandas DataFrame for further analysis
        df = pd.DataFrame(data_by_id)
        return df
    except FileNotFoundError as e:
        print(f"Error occured:\n{e}")
    except ValueError as e:
        print(f"Error occured:\n{e}")
    except json.JSONDecodeError as e:
        print(f"Error occured:\n{e}")
    except Exception as e:
        print(f"Error occured:\n{e}")
# --- Execute transformation ---
# Example usage
# try:
#     df = Transform_seriesdata_by_id(r'data/raw_seriesdata_byID.json')
#     print(df.head())
#     print(df.shape)
# except Exception as e:
#     print(f'Error occured:\n{e}')
