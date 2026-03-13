import requests

url = r"https://cancer-data.p.rapidapi.com/dataListIndex"
queryString = {"value":"0","index":"0","orderBy":"asc","limit":"500"}
headers = {
	"x-rapidapi-key": "0bc6596902msh3e6ec4940e271bbp1e4fa8jsn48fb9f4c4553",
	"x-rapidapi-host": "cancer-data.p.rapidapi.com",
	"Content-Type": "application/json"
}
response = requests.get(url, headers=headers, params=queryString)

print(response.status_code)