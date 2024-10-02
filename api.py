from dotenv import load_dotenv
import json
import os
import requests
import csv

from datetime import datetime

load_dotenv()


def email_update_json(table_name, api_count, api_date, file_path='email.json'):
    try:
        # Load existing data from the file if it exists
        with open(file_path, 'r') as file:
            file_data = json.load(file)
    except FileNotFoundError:
        file_data = {}
    if table_name in file_data:
        # Update the existing entry
        file_data[table_name]['api_count'] = api_count
        file_data[table_name]['api_date'] = api_date
        
    with open(file_path, 'w') as file:
        json.dump(file_data, file, indent=4)

now = formatDateTime = None
try:
    now = datetime.now()
    formatDateTime = now.strftime("%d/%m/%Y %H:%M")
    formatted_date = now.strftime("%Y-%m-%d")
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

endpoints = [GetCaseCustomProperties]
print(len(endpoints))

endpoints_dict = {
    GetCalls: 'Call', GetCampaigns: 'Campaign', GetCases: 'Case', GetCaseCustomProperties: 'CaseCustomPropertie',
    GetCaseTickets: 'CaseTicket', GetEmailMessages: 'EmailMessage', GetRecords: 'Record', GetRecordGroups: 'RecordGroup',
    GetRecordItems: 'RecordItem', GetRecordItemDefinitions: 'RecordItemDefinition', GetScriptitems: 'ScriptItem', GetScriptitemDefinitions: 'ScriptItemDefinition',
    GetSmsMessages: 'SmsMessage', GetTeams: 'Team', GetUsers: 'User', GetUserToTeams: 'UserToTeam',
    GetWorkflows: 'Workflow', GetWorkflowStates: 'WorkflowState', GetWorkflowStateHistories: 'WorkflowStateHistory', GetUserWorkStats: 'UserWorkState'
}



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

errs_number = 0

#plik ze statusem od kiedy dane maja pobierac sie do danej tabeli
with open('import_status.json', 'r') as file:
    data = json.load(file)

# Create a new dictionary with table names as keys and last_success_date as values
table_date_dict = {key: value['last_success_date'] for key, value in data.items()}

api_db_compare = {}
with open ("pobrane_dane_licznik.txt", 'a', encoding='utf-8') as file:
    file.write(f'{formatDateTime}\n')
    for url, table_name_endpoint in endpoints_dict.items():
        table_import_date = table_date_dict[table_name_endpoint]
        urlsplit = url.split('/')[-1].split('?')[0]
        csvname = f'csv\{urlsplit}.csv'
        #tutaj url += data z pliku
        #od from pobiera włącznie do dzisiaj (nie)włącznie
        url += f"?from={table_import_date}&to={formatted_date}"
        #url += f"?from=2024-08-01&to=2024-08-13"
        response = requests.get(url,headers=headers,json=data)  
        if response.status_code == 200:
            data = response.json()
        
            api_db_compare[table_name_endpoint] = len(data)
            
            file.write(f'{url} - {len(data)}\n')
            print(f'{url} - {len(data)}\n')
            #print("wchodze do csv")
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
            #print("koniec csv")
        else:
            errs_number += 1
            file.write(f'{url} - blad {response.status_code}\n')
            print(f'{url} - blad {response.status_code}\n')
    file.write(f'Liczba błędów: {errs_number}\n')

for k,v in api_db_compare.items():
    email_update_json(k,v,formatted_date)
