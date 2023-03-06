
import psycopg2
import os
import sys
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

# Database connection details --Note: Dont do like this in production code


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


def connection():
    conn_params_dic = {
        "host": hostname,
        "database": database,
        "user": username,
        "password": pwd
    }

    conn = None
    try:
        print('Connecting to the PostgreSQL...........')
        conn = psycopg2.connect(**conn_params_dic)
        print("Connection successful..................")

    except psycopg2.OperationalError as err:
        # pass exception to function
        show_psycopg2_exception(err)

        # set the connection to 'None' in case of error
        conn = None

    return conn


# As all df have 'country' column we use that columns to join df
JOIN_ON_COLUMNS = ['country']
JOIN_TYPE = "left"

SPEC_COLS = [
    "country",
    "country_name",
    "region",
    "surface_area",
    "independence_year",
    "country_population",
    "life_expectancy",
    "local_name",
    "head_of_state",
    "capital",
    "country_2",
    "city_id",
    "city_name",
    "city_district",
    "city_population",
    "language",
    "is_official_language",
    "language_percentage"
]


CITY_COL_DICT = {
    "ID": "city_id",
          "Name": "city_name",
          "CountryCode": "country",
          "District": "city_district",
          "Population": "city_population"
}


COUNTRY_COL_DICT = {
    "Code": "country",
    "Name": "country_name",
    "Continent": "continent",
    "Region": "region",
    "SurfaceArea": "surface_area",
    "IndepYear": "independence_year",
    "Population": "country_population",
    "LifeExpectancy": "life_expectancy",
    "GNP": "gross_national_product",
    "GNPOld": "old_gross_national_product",
    "LocalName": "local_name",
    "GovernmentForm": "government_form",
    "HeadOfState": "head_of_state",
    "Capital": "capital",
    "Code2": "country_2"
}


COUNTRY_LANGUAGE_COL_DICT = {
    "CountryCode": "country",
    "Language": "language",
    "IsOfficial": "is_official_language",
    "Percentage": "language_percentage"
}
