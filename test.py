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

from datetime import datetime

now = formatDateTime = formatted_date = None
try:
    now = datetime.now()
    formatDateTime = now.strftime("%d/%m/%Y %H:%M")
    formatted_date = now.strftime("%Y-%m-%d")
except Exception as e:
    pass

from_address = os.getenv('from_address')
to_address_str = os.getenv('to_address')
password = os.getenv('password')
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