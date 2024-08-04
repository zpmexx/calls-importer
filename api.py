from dotenv import load_dotenv
import json
import os
import requests
import csv
load_dotenv()

username = os.getenv('username')
login = os.getenv('login')
user_password = os.getenv('user_password')
account_url = os.getenv('account_url')
getcase_url = os.getenv('getcase_url')

headers = {
    'Content-Type': 'application/json'
}
data = {
    "login": login,
    "password": user_password
}

response = requests.post(account_url, headers=headers, json=data)

if response.status_code == 200:
    # Process the data
    data = response.json()
    #print(data)
    token = data['accessToken']
else:
    print("No token")
    
headers["Authorization"] = f"Bearer {token}"
            
response = requests.get(getcase_url,headers=headers,json=data)  

if response.status_code == 200:
    data = response.json()
    print(len(data))
    with open('getcase.csv', mode='w', newline='') as file:
        
        writer = csv.writer(file)
        
        header = list(data[0].keys())
        writer.writerow(header)
        
        # Write the values for all records
        for item in data:
            values = list(item.values())
            writer.writerow(values)
            
#GetCalls
getcalls_url = os.getenv('getcalls_url')
response = requests.get(getcalls_url,headers=headers,json=data)  

if response.status_code == 200:
    data = response.json()
    print(len(data))
    with open('getcalls.csv', mode='w', newline='') as file:
        
        writer = csv.writer(file)
        
        header = list(data[0].keys())
        writer.writerow(header)
        
        # Write the values for all records
        for item in data:
            values = list(item.values())
            writer.writerow(values)
else:
    print(f"blad: {response.status_code}")
