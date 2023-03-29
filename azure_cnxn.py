import pyodbc

server = 'TOBOLA-SGP'
database = 'TOBOLA'
driver = '{ODBC Driver 17 for SQL Server}'
trusted_connection = 'yes'

cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';Trusted_Connection='+trusted_connection+';')
cnxn_string = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';Trusted_Connection='+trusted_connection+';'

# Create a cursor
cursor = cnxn.cursor()

