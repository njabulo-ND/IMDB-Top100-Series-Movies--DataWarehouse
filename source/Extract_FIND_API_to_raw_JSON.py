import requests
import json
url = "https://imdb146.p.rapidapi.com/v1/find/"

querystring = {"query":"brad"}

headers = {
	"x-rapidapi-key": "0bc6596902msh3e6ec4940e271bbp1e4fa8jsn48fb9f4c4553",
	"x-rapidapi-host": "imdb146.p.rapidapi.com",
	"Content-Type": "application/json"
}

response = requests.get(url, headers=headers, params=querystring)
print(response.status_code)
data = response.json()
with open(r"data\raw_data.json",'w') as raw_data:
    json.dump(data,raw_data,indent=4)
