from dotenv import load_dotenv
import json
import os
import requests
import csv

from datetime import datetime

load_dotenv()

now = formatDateTime = None
try:
    now = datetime.now()
    formatDateTime = now.strftime("%d/%m/%Y %H:%M")
except Exception as e:
    pass


username1 = os.getenv('username1')
login = os.getenv('login')
user_password = os.getenv('user_password')
account = os.getenv('account')
GetCalls = os.getenv('GetCalls')
GetCampaigns = os.getenv('GetCampaigns')
GetCases = os.getenv('GetCases')
GetCaseCustomProperties = os.getenv('GetCaseCustomProperties')
GetCaseTickets = os.getenv('GetCaseTickets')
GetEmailMessages = os.getenv('GetEmailMessages')
GetRecords = os.getenv('GetRecords')
GetRecordGroups = os.getenv('GetRecordGroups')
GetRecordItems = os.getenv('GetRecordItems')
GetRecordItemDefinitions = os.getenv('GetRecordItemDefinitions')
GetScriptitems = os.getenv('GetScriptitems')
GetScriptitemDefinitions = os.getenv('GetScriptitemDefinitions')
GetSmsMessages = os.getenv('GetSmsMessages')
GetTeams = os.getenv('GetTeams')
GetUsers = os.getenv('GetUsers')
GetUserToTeams = os.getenv('GetUserToTeams')
GetWorkflows = os.getenv('GetWorkflows')
GetWorkflowStates = os.getenv('GetWorkflowStates')
GetWorkflowStateHistories = os.getenv('GetWorkflowStateHistories')
GetUserWorkStats = os.getenv('GetUserWorkStats')

endpoints = [GetCalls,GetCampaigns,GetCases,GetCaseCustomProperties,GetCaseTickets,GetEmailMessages,GetRecords,
             GetRecordGroups,GetRecordItems,GetRecordItemDefinitions,GetScriptitems,GetScriptitemDefinitions,
             GetSmsMessages,GetTeams,GetUsers,GetUserToTeams,GetWorkflows,GetWorkflowStates,GetWorkflowStateHistories,GetUserWorkStats]


print(len(endpoints))




headers = {
    'Content-Type': 'application/json'
}
data = {
    "login": login,
    "password": user_password
}

response = requests.post(account, headers=headers, json=data)

if response.status_code == 200:
    # Process the data
    data = response.json()
    #print(data)
    token = data['accessToken']
else:
    print("No token")
headers["Authorization"] = f"Bearer {token}"
with open ("licznik.txt", 'w') as file:
    file.write(f'{formatDateTime}\n')
    for url in endpoints:
        urlsplit = url.split('/')[-1].split('?')[0]
        csvname = f'csv\{urlsplit}.csv'
        #tutaj url += data z pliku
        response = requests.get(url,headers=headers,json=data)  
        if response.status_code == 200:
            data = response.json()
            file.write(f'{url} - {len(data)}\n')
            print(f'{url} - {len(data)}\n')

            
            with open(csvname, mode='w', newline='') as csv_file:
                writer = csv.writer(csv_file)
                try:
                    header = list(data[0].keys())
                    writer.writerow(header)
                    
                    # Write the values for all records
                    for item in data:
                        values = list(item.values())
                        writer.writerow(values)
                except:
                    pass
        else:
            file.write(f'{url} - blad {response.status_code}\n')
            print(f'{url} - blad {response.status_code}\n')

                