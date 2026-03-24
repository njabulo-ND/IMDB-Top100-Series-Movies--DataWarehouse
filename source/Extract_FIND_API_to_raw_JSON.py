import requests
import json
import os
from dotenv import load_dotenv


# Code snippet from rapidapi
project_dir = os.getcwd()
env_path =  os.path.join(project_dir,".env")
url = "https://imdb-top-100-movies.p.rapidapi.com/"
load_dotenv(dotenv_path=env_path)
print(os.getenv('API_KEY'))
headers = {
	"x-rapidapi-key":os.getenv('API_KEY'),
	"x-rapidapi-host": "imdb-top-100-movies.p.rapidapi.com",
	"Content-Type": "application/json"
}

# Calling out the Find API
def extractingFromFIND(url, headers): 
    try:
        response = requests.get(url, headers=headers)
        print(response.status_code)
        data = response.json()
        # Storing the data to the raw_data_json
        with open(r"data\raw_data.json", 'w') as raw_data:
            json.dump(data, raw_data, indent=4)
    except requests.RequestException as e:
        print(f"Error occured:\n{e}")
    except FileNotFoundError:
        print(f'File not found')
    except json.JSONDecodeError as e:
        print(f'Error occured:\n{e}')
    except Exception as e:
        print(f'Error occured:\n{e}')
        
extractingFromFIND(url,headers)
