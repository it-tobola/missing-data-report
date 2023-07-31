import pyodbc
from sqlalchemy.engine import URL
import sqlalchemy as sql

server = 'localhost'
database = 'TOBOLA'
driver = '{ODBC Driver 17 for SQL Server}'
trusted_connection = 'yes'

cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';Trusted_Connection='+trusted_connection+';')
cnxn_string = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';Trusted_Connection='+trusted_connection+';'

# Create a cursor
cursor = cnxn.cursor()

cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": cnxn_string})
engine = sql.create_engine(cnxn_url)
