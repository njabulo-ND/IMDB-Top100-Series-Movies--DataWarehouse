import requests
import json
import os
from dotenv import load_dotenv


# Fetching key from env 
try:
    project_dir = os.getcwd()
    env_path = os.path.join(project_dir, ".env")
    load_dotenv(dotenv_path=env_path)
    if os.getenv('ALT_API_KEy'):
        print('Key found!')
    else:
        raise OSError('Key no found!!')
except OSError as e:
    print(f'OS error related occured: \n{e}')
except Exception as e:
    print(f"Error occured:\n{e}")


#Movie code snippet from rapidapi
movies_url = "https://imdb-top-100-movies.p.rapidapi.com/"
movies_headers = {
    "x-rapidapi-key": os.getenv('ALT_API_KEY'),
    "x-rapidapi-host": "imdb-top-100-movies.p.rapidapi.com",
    "Content-Type": "application/json"
}


""" Calling out the top100 movie list API"""
def extracting_top100_movie_list(url, headers):
    """
    Fetches movie data from the IMDb API using a given URL and headers,
    then stores the raw JSON response locally.

    Parameters:
    - url (str): Full API endpoint to request data from
    - headers (dict): Request headers including authentication keys

    Returns:
    - None (writes JSON response to 'data/raw_data.json')
    """
    try:
        response = requests.get(url, headers=headers)
        print(response.status_code)
        data = response.json()
        # Storing the data to the raw_data_json
        with open(r"data\raw_data_movies_top_100.json", 'w') as raw_data:
            json.dump(data, raw_data, indent=4)
    except requests.RequestException as e:
        print(f"Error occured:\n{e}")
    except FileNotFoundError:
        print(f'File not found')
    except json.JSONDecodeError as e:
        print(f'Error occured:\n{e}')
    except Exception as e:
        print(f'Error occured:\n{e}')
# Example usage
# extracting_top100_movie_list(url,headers)


""" Calling out the movie by id list API"""
def extracting_moviedata_by_id(url, list_of_ids, headers):
    """
    Fetch movie data by ID from the RapidAPI IMDb Top 100 API.

    Parameters:
    - url: str, base API URL (without 'topX' endpoint)
    - list_of_ids: list of int or str, movie IDs to fetch (1-100)
    - headers: dict, headers including RapidAPI key

    Returns:
    - list of dicts containing movie data
    """
    try:
        # Validation checks
        _,tail = os.path.split(url)
        print(tail)
        if 'top' in tail :
            raise ValueError(f'URL must not have endpoint, only the domain')
        if not all(str(id).isnumeric() for id in list_of_ids):
            raise ValueError(f'Ids are not numeric must be any number from 1 - 100')
        if all(int(id) > 100 for id in list_of_ids):
            raise ValueError(f'Id numbers above 100 are not available from the top 100 series')
        
        # Fetch each movie by ID
        list_for_responses = []
        for ticker in list_of_ids:
            each_url = f"{url}top{ticker}"
            response = requests.get(each_url, headers=headers)
            if response.status_code == 200:
                data_by_id = response.json()
                list_for_responses.append(data_by_id)
            else:
                print(f'Id: {ticker} was not found\nWarning: Data will be short!')
        return list_for_responses
    except requests.RequestException as e:
        print(f"Error occured:\n{e}")
    except FileNotFoundError:
        print(f'File not found')
    except json.JSONDecodeError as e:
        print(f'Error occured:\n{e}')
    except Exception as e:
        print(f'Error occured:\n{e}')
# Example usage
# --- Save data to file ---
#list_of_movies_ids_to_fetch = [1, 2, 3, 4, 5]
#try:
#    with open(r"data/raw_moviedata_byID.json", 'w') as opened_file:
#        data = extracting_moviedata_by_id(movies_url, list_of_movies_ids_to_fetch,movies_headers)
#        json.dump(data, opened_file, indent=4)
#        if len(data) == len(list_of_series_ids_to_fetch):
#            print(f'Data loaded successfully')
#        else:
#            print('Data is short there was an ID that was not found')
#except FileNotFoundError:
#    print(f'File not found')
#except json.JSONDecodeError as e:
#    print(f'Error occured:\n{e}')
#except Exception as e:
#    print(f'Error occured:\n{e}')



#Series code snippet
series_url = "https://imdb-top-100-movies.p.rapidapi.com/series/"

series_headers = {
	"x-rapidapi-key":os.getenv('ALT_API_KEY'),
	"x-rapidapi-host": "imdb-top-100-movies.p.rapidapi.com",
	"Content-Type": "application/json"
}


""" Calling out the top100 series list API"""
def extracting_top100_series_list(url, headers):
    """
    Fetches movie data from the IMDb API using a given URL and headers,
    then stores the raw JSON response locally.

    Parameters:
    - url (str): Full API endpoint to request data from
    - headers (dict): Request headers including authentication keys

    Returns:
    - None (writes JSON response to 'data/raw_data.json')
    """
    try:
        response = requests.get(url, headers=headers)
        print(response.status_code)
        data = response.json()
        # Storing the data to the raw_data_json
        with open(r"data\raw_data_series_top_100.json", 'w') as raw_data:
            json.dump(data, raw_data, indent=4)
    except requests.RequestException as e:
        print(f"Error occured:\n{e}")
    except FileNotFoundError:
        print(f'File not found')
    except json.JSONDecodeError as e:
        print(f'Error occured:\n{e}')
    except Exception as e:
        print(f'Error occured:\n{e}')
# Example usage
# extracting_top100_series_list(series_url, series_headers)



""" Calling out the series by id list API"""
def extracting_seriesdata_by_id(url, list_of_ids, headers):
    """
    Fetch series data by ID from the RapidAPI IMDb Top 100 API.

    Parameters:
    - url (str): Base API URL (without 'topX' endpoint)
    - list_of_ids (list of int or str): Series IDs to fetch (1–100)
    - headers (dict): Request headers including RapidAPI key

    Returns:
    - list of dicts containing series data
    """
    try:
        # Validation checks
        _,tail = os.path.split(url)
        if 'top' in tail :
            raise ValueError(f'URL must not have endpoint, only the domain')
        if not all(str(id).isnumeric() for id in list_of_ids):
            raise ValueError(f'Ids are not numeric must be any number from 1 to 100')
        if all(int(id) > 100 for id in list_of_ids):
            raise ValueError(f'Id numbers above 100 are not available from the top 100 series table')
        
        # Fetch each movie by ID
        list_for_responses = []
        for ticker in list_of_ids:
            each_url = f"{url}top{ticker}"
            response = requests.get(each_url, headers=headers)
            if response.status_code == 200:
                data_by_id = response.json()
                list_for_responses.append(data_by_id)
            else:
                print(f'Id: {ticker} was not found\nWarning: Data will be short!')
        return list_for_responses
    except requests.RequestException as e:
        print(f"Error occured:\n{e}")
    except FileNotFoundError:
        print(f'File not found')
    except json.JSONDecodeError as e:
        print(f'Error occured:\n{e}')
    except Exception as e:
        print(f'Error occured:\n{e}')
# Example usage
# --- Save data to file ---
# list_of_series_ids_to_fetch = [1, 39, 33, 50, 100]
# try:
#    with open(r"data/raw_seriesdata_byID.json", 'w') as opened_file:
#        data = extracting_seriesdata_by_id(series_url, list_of_series_ids_to_fetch,series_headers)
#        json.dump(data, opened_file, indent=4)
#        if len(data) == len(list_of_series_ids_to_fetch):
#             print(f'Data loaded successfully')
#        else:
#             print('Data is short there was an ID that was not found')
# except FileNotFoundError:
#     print(f'File not found')
# except json.JSONDecodeError as e:
#     print(f'Error occured:\n{e}')
# except Exception as e:
#     print(f'Error occured:\n{e}')

