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

irisData = pd.read_csv(
    'https://raw.githubusercontent.com/Muhd-Shahid/Learn-Python-Data-Access/main/iris.csv', index_col=False)
irisData.dtypes


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

# Define a function that handles and parses psycopg2 exceptions


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

# Define a connect function for PostgreSQL database server


def connect(conn_params_dic):
    conn = None
    try:
        print('Connecting to the PostgreSQL...........')
        conn = psycopg2.connect(**conn_params_dic)
        print("Connection successfully..................")

    except OperationalError as err:
        # passing exception to function
        show_psycopg2_exception(err)
        # set the connection to 'None' in case of error
        conn = None
    return conn

# Define function using cursor.executemany() to insert the dataframe


def execute_many(conn, datafrm, table):

    # Creating a list of tupples from the dataframe values
    tpls = [tuple(x) for x in datafrm.to_numpy()]

    # dataframe columns with Comma-separated
    cols = ','.join(list(datafrm.columns))

    # SQL query to execute
    sql = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(sql, tpls)
        conn.commit()
        print("Data inserted using execute_many() successfully...")
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function
        show_psycopg2_exception(err)
        cursor.close()


# Connect to the database
conn = connect(conn_params_dic)
conn.autocommit = True
# Run the execute_many method
execute_many(conn, irisData, 'iris')
# Close the connection
conn.close()

# Connect to the database
conn = connect(conn_params_dic)
cursor = conn.cursor()

# Execute query
sql = "SELECT * FROM iris"
cursor.execute(sql)

# Fetch all the records
tuples = cursor.fetchall()

# list of columns
cols = list(irisData.columns)

irisdf = pd.DataFrame(tuples, columns=cols)
print()
print(irisdf.head())

# Close the cursor
cursor.close()

# Close the conn
conn.close()
