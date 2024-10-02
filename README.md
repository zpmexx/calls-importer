# Calls import for CDRL

1. First part of a script is api.py:
	- Read endpoints
	- Authorize via login/password + bearer token
	- Import_status.json - file that contains information about latest successful import date
	- For every endpoint , write csv that contains the data
	- Save successful api download for every endpoint into email.json
2. Second part is import.py:
	- Connect to db
	- For every csv file insert/update data into db
	- Write into email.json db import status
	- Compare db import status with downloaded data from api
	- Send an email with a final summary