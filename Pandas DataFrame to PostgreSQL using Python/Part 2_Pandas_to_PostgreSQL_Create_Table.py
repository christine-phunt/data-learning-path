# import sys to get more detailed Python exception info
import pandas as pd
import psycopg2.extras as extras
from psycopg2 import OperationalError, errorcodes, errors
import psycopg2
import sys

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
load_dotenv()

# import the connect library for psycopg2
# import the error handling libraries for psycopg2

hostname = os.getenv('hostname')
database = os.getenv('database')
username = os.getenv('username')
pwd = os.getenv('pwd')
port_id = os.getenv('port')

# Note: please change your database, username & password as per your own values
conn_params_dic = {
    "host": hostname,
    "database": database,
    "user": username,
    "password": pwd
}


def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occured
    line_n = traceback.tb_lineno

    # print the connect() error
    print("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print("psycopg2 traceback:", traceback, "-- type:", err_type)

    # psycopg2 extensions.Diagnostics object attribute
    print("\nextensions.Diagnostics:", err.diag)

    # print the pgcode and pgerror exceptions
    print("pgerror:", err.pgerror)
    print("pgcode:", err.pgcode, "\n")


def connect(conn_params_dic):
    conn = None
    try:
        print('Connecting to the PostgreSQL...........')
        conn = psycopg2.connect(**conn_params_dic)
        print("Connection successful..................")

    except OperationalError as err:
        # pass exception to function
        show_psycopg2_exception(err)

        # set the connection to 'None' in case of error
        conn = None

    return conn


irisData = pd.read_csv(
    'https://github.com/Muhd-Shahid/Write-Raw-File-into-Database-Server/raw/main/iris.csv', index_col=False)
irisData.head()
irisData.dtypes

conn = connect(conn_params_dic)
conn.autocommit = True

if conn != None:

    try:
        cursor = conn.cursor()
        # Dropping table iris if exists
        cursor.execute("DROP TABLE IF EXISTS iris;")

        sql = '''CREATE TABLE iris(
        sepal_length DECIMAL(2,1) NOT NULL, 
        sepal_width DECIMAL(2,1) NOT NULL, 
        petal_length DECIMAL(2,1) NOT NULL, 
        petal_width DECIMAL(2,1),
        species CHAR(11)NOT NULL
        )'''

        # Creating a table
        cursor.execute(sql)
        print("iris table is created successfully..................")

        # Closing the cursor & connection
        cursor.close()
        conn.close()

    except OperationalError as err:
        # pass exception to function
        show_psycopg2_exception(err)
        # set the connection to 'None' in case of error
        conn = None
