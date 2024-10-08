import pyodbc
import csv
import os
import chardet
import json

from datetime import datetime
from dotenv import load_dotenv
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

#load env file
load_dotenv()

from_address = os.getenv('from_address')
to_address_str = os.getenv('to_address')
password = os.getenv('password')

def email_update_json(table_name, db_count, db_date, file_path='email.json'):
    try:
        # Load existing data from the file if it exists
        with open(file_path, 'r') as file:
            file_data = json.load(file)
    except FileNotFoundError:
        file_data = {}
    if table_name in file_data:
        # Update the existing entry
        file_data[table_name]['db_count'] = db_count
        file_data[table_name]['db_date'] = db_date
        
    with open(file_path, 'w') as file:
        json.dump(file_data, file, indent=4)

def write_or_update_json(table_name, last_success_date, file_path='import_status.json'):
    try:
        # Load existing data from the file if it exists
        with open(file_path, 'r') as file:
            file_data = json.load(file)
    except FileNotFoundError:
        file_data = {}

    # Check if the table already exists in the file

    if table_name in file_data:
        # Update the existing entry
        file_data[table_name]['last_success_date'] = last_success_date
        print(f"Updated {table_name} last_success_date to {last_success_date}")
    else:
        print(f"nie znaleziono {table_name}")
        print(table_name)
        print(file_data)
        # # Add a new entry
        # file_data[table_name] = {
        #     "last_success_date": last_success_date
        # }
        # print(f"Added new entry for {table_name} with last_success_date {last_success_date}")

    # Write updated data back to the JSON file
    with open(file_path, 'w') as file:
        json.dump(file_data, file, indent=4)


def delete_csv_files(folder_path):
    # Iterate over all files in the directory
    for filename in os.listdir(folder_path):
        # Construct the full file path
        file_path = os.path.join(folder_path, filename)
        
        # Check if it's a .csv file and delete it
        if filename.endswith('.csv') and os.path.isfile(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                with open ('logfile.log', 'a') as logfile:
                    logfile.write(f"problem z usunieciem plikow csv\n")

def escape_identifier(identifier):
    return f"[{identifier}]"


# Database connection details
# CHANGE THIS WITH NEXT PUSH
server = 'cdrl-bidata02'
database = 'systel'

connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
from datetime import datetime

now = formatDateTime = formatted_date = None
try:
    now = datetime.now()
    formatDateTime = now.strftime("%d/%m/%Y %H:%M")
    formatted_date = now.strftime("%Y-%m-%d")
except Exception as e:
    pass
#Connect to the database
try:
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    print('git')
    with open ('logfile.log', 'a') as logfile:
        logfile.write(f"{formatDateTime}\n")
except Exception as e:
    with open ('logfile.log', 'a') as logfile:
        logfile.write(f"{formatDateTime}\nProblem z polaczeniem z baza danych\n")



folder_path = 'csv'
final_dict = {}
api_db_compare = {}
# Loop over each file in the directory
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'): 
        # Path to the CSV file
        csv_file_path = os.path.join('csv', filename)

        table_name = filename.replace('Get', '').replace('s.csv', '')
        
        #poprawa nazw tabel które automatycznie zwracają błąd
        if table_name == "WorkflowStateHistorie":
            table_name = "WorkflowStateHistory"
            
        elif table_name == "UserWorkStat":
            table_name = 'UserWorkState'
            
        elif table_name == "Scriptitem":
            table_name = 'ScriptItem'
            
        elif table_name == "ScriptitemDefinition":
            table_name = 'ScriptItemDefinition'
        
            
        # with open(csv_file_path, 'rb') as file:
        #     raw_data = file.read()
        #     result = chardet.detect(raw_data)
        #     encoding = result['encoding']

        # print(f"Kodowanie pliku {csv_file_path} to: {encoding}")
        
        print(f'Przetwarzam plik {filename} tabela {table_name}\n')
        # Open the CSV file and read data with ISO-8859-1 encoding
        with open(csv_file_path, mode='r', newline='', encoding='Windows-1252') as file:
            csv_reader = csv.DictReader(file)
            row_count = 0
            try:
                columns = [column for column in csv_reader.fieldnames]
                escape_columns = [escape_identifier(column) for column in csv_reader.fieldnames]
                escaped_table_name = escape_identifier(table_name)
                
            # pusty plik csv, brak danych do wgrania 
            except Exception as e:
                print("brak danych do wgrania\n")
                api_db_compare[table_name] = 0
                if formatted_date:
                    write_or_update_json(table_name, formatted_date)
                    final_dict[filename] = 0
                continue
            try:
                # Extract column names from the header
                # Prepare the SQL INSERT statement
                columns_placeholder = ', '.join(escape_columns)
                values_placeholder = ', '.join(['?'] * len(columns))
                insert_query = f"INSERT INTO {escaped_table_name} ({columns_placeholder}) VALUES ({values_placeholder})"
                
                pk_column = escape_columns[0]  # Assuming the first column is the primary key
                update_columns = ', '.join([f"{col} = ?" for col in escape_columns[1:]])
                update_query = f"UPDATE {escaped_table_name} SET {update_columns} WHERE {pk_column} = ?"

                for row in csv_reader:
                    row_count += 1
                    # Extract values from the row and handle empty fields
                    values = [row[column] or None for column in columns]
                    try:
                        # Attempt to insert the row
                        cursor.execute(insert_query, values)
                    except pyodbc.IntegrityError as e:
                        if 'PRIMARY KEY' in str(e):  # Handle primary key conflict
                            print(f"konflikt. Update. {e}")
                            with open ('logfile.log', 'a') as logfile:
                                logfile.write(f"Konflikt ID na tabeli {table_name} klucz {pk_column} - {e}\n")
                                
                            # If insert fails, attempt to update
                            update_values = values[1:] + [values[0]]  # Exclude PK from SET clause, add PK to WHERE clause
                            cursor.execute(update_query, update_values)
                        else:
                            #IntegrityError inny niz PK
                            with open ('logfile.log', 'a') as logfile:
                                logfile.write(f"Bład z importem danych (IntegrityError) do tabeli {table_name} - {e}\n")
                            raise  # Reraise if it's not a primary key conflict
                    except Exception as e:
                        with open('logfile.log', 'a') as logfile:
                            logfile.write(f"Problem z importem do tabeli {table_name} - {e}\n")
                        raise  # Optionally reraise the exception if you want to stop execution

                
                api_db_compare[table_name] = row_count
                
                if row_count > 0:
                    conn.commit()
                    print(f"Wgrano pomyślnie do tabeli {table_name} {row_count} wierszy.\n")
                    if formatted_date:
                        write_or_update_json(table_name, formatted_date)
                
                ## execute many import
                #data_to_insert = []
                # for row in csv_reader:
                #     # Extract values from the row and handle empty fields
                #     values = [row[column] or None for column in columns]
                    
                #     # Append the row data to the list
                #     data_to_insert.append(values)
                #     row_count += 1
                
                # api_db_compare[table_name] = row_count
                
                # if row_count > 0:
                # # Execute the batch insert
                #     cursor.executemany(insert_query, data_to_insert)
                #     conn.commit()
                #     print(f"Wgrano pomyślnie do tabeli {table_name} {row_count} wierszy.\n")
                #     if formatted_date:
                #         write_or_update_json(table_name, formatted_date)
                
            except Exception as e:
                with open ('logfile.log', 'a') as logfile:
                    logfile.write(f"Bład z plikiem {filename} - {e}\n")

        final_dict[filename] = row_count
 
cursor.close()
conn.close()       

try: 
    with open ('wgrane_dane_db_licznik.txt', 'a') as file:
        file.write(f'{formatDateTime}\n')
        for k,v in final_dict.items():
            file.write(f'{k} - {v}\n')
        file.write("\n")
except Exception as e:
    with open ('logfile.log', 'a') as logfile:
        logfile.write(f"Problem z wgraniem do pliku wgrane_date - {e}\n")
        
try:
    delete_csv_files(folder_path)
except Exception as e:
    with open ('logfile.log', 'a') as logfile:
        logfile.write(f"Bład z usuwaniem plików z folderu - {e}\n")
        
try:
    for k,v in api_db_compare.items():
        email_update_json(k,v,formatted_date)
except Exception as e:
    with open ('logfile.log', 'a') as logfile:
        logfile.write(f"Problem z zapisem do pliku email_podsumowanie - {e}\n")
    
#wysylanie maila z podsumowaniem

body = ''
errors_count = 0
#zczytywanie podsumowanie z pliku email.json
try:
    json_file_path = 'email.json'
    # Read the JSON file
    with open(json_file_path, 'r') as file:
        data = json.load(file)
        
    for table_name, parameters in data.items():
        if parameters['api_date'] == parameters['db_date'] and parameters['api_count'] == parameters['db_count']:
            body += f"""<h5 style="color:green"> OK - Tabela {table_name} - data api {parameters['api_date']} data db {parameters['db_date']} count api {parameters['api_count']} count db {parameters['db_count']}</h5>\n"""
        else:
            body += f"""<h5 style="color:red">BŁĄD - Tabela {table_name} - data api {parameters['api_date']} data db {parameters['db_date']} count api {parameters['api_count']} count db {parameters['db_count']}</h5>\n"""
            errors_count += 1
    body = f"<h1>LICZBA BŁĘDÓW: {errors_count}</h1>\n" + body
except Exception as e:
    with open ('logfile.log', 'a') as logfile:
        logfile.write(f"Problem z importem pliku email.json - {e}\n")
        
#wysylka maila
try:
    to_address = json.loads(to_address_str)
    msg = MIMEMultipart()
    msg['From'] = from_address
    msg["To"] = ", ".join(to_address)
    msg['Subject'] = f"Import danych systel {formatDateTime}."
    
except Exception as e:
    with open ('logfile.log', 'a') as file:
        file.write(f""" Problem z wysłaniem email - {str(e)}\n""")

msg.attach(MIMEText(body, 'html'))
try:
    server = smtplib.SMTP('smtp-mail.outlook.com', 587)
    server.starttls()
    server.login(from_address, password)
    text = msg.as_string()
    server.sendmail(from_address, to_address, text)
    server.quit()    
except Exception as e:
    with open ('logfile.log', 'a') as file:
        file.write(f"""{formatDateTime} Problem z wysłaniem maila - {str(e)}\n""")