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

# print(conn_params_dic)


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
# Method 2 : Using psycopg2
# Define postgresql_to_dataframe function to load data into a pandas # dataframe


def postgresql_to_dataframe(conn, sql, col_names):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except (Exception, psycopg2.DatabaseError) as err:
        # passing exception to function
        show_psycopg2_exception(err)

    # Naturally we get a list of tupples
    tupples = cursor.fetchall()
    cursor.close()

    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tupples, columns=col_names)
    return df


# Method 3 : Using Alchemy
# Define using_alchemy function to load data into a pandas # dataframe
connect_alchemy = "postgresql+psycopg2://%s:%s@%s/%s" % (
    conn_params_dic['user'],
    conn_params_dic['password'],
    conn_params_dic['host'],
    conn_params_dic['database']
)


def using_alchemy(sql):
    try:
        engine = create_engine(connect_alchemy)
        df = pd.read_sql_query(sql, con=engine)
    except OperationalError as err:
        # passing exception to function
        show_psycopg2_exception(err)
    return df


# Connecting to PostgreSQL Data
conn = connect(conn_params_dic)
col_names = ['sepal_length', 'sepal_width',
             'petal_length', 'petal_width', 'species']
# Create a statement for querying PostgreSQL.
sql = "select * from iris"

df = postgresql_to_dataframe(conn, sql, col_names)
print()
print(df.head())

conn = connect(conn_params_dic)
df = pd.read_sql_query(sql, con=conn)
df.head()

# df = using_alchemy(sql)
# df.head()
