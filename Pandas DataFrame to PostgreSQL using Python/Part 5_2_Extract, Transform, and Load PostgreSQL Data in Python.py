# import sys to get more detailed Python exception info
import sys
# import the connect library for psycopg2
import psycopg2
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors
import psycopg2.extras as extras
from sqlalchemy import create_engine
import pandas as pd
# Extract Trabsforl & Load
import petl as etl

import os
from dotenv import load_dotenv
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
        print("Connection successful..................")

    except OperationalError as err:
        # passing exception to function
        show_psycopg2_exception(err)
        # set the connection to 'None' in case of error
        conn = None
    return conn


# Connecting to PostgreSQL Data
conn = connect(conn_params_dic)

# Create a SQL Statement to Query PostgreSQL
# sql = "SELECT * FROM iris WHERE species = 'testing'"
sql = "SELECT * FROM iris "

extractData = etl.fromdb(conn, sql)

extractData.head()

table2 = etl.addfield(extractData, 'source', 'iris.csv')
joined_table = etl.join(extractData, table2, key='species')
# Display the result
print(joined_table)

transformData = etl.sort(extractData, 'species')

# etl.tocsv(transformData, 'iris_v1.csv')
filled_table = etl.filldown(extractData, 'petal_width')
# Display the result
print(filled_table)

transposed_table = etl.transpose(extractData)
# Display the result
print(transposed_table)

# save to file
etl.tocsv(joined_table, 'iris_joined_table.csv')
etl.tocsv(transformData, 'iris_transformData.csv')
etl.tocsv(filled_table, 'iris_filled_table.csv')
etl.tocsv(transposed_table, 'iris_transposed_table.csv')
