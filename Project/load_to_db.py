import argparse
import io
import os
from datetime import datetime
from pyarrow import parquet as pq
import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError, errorcodes, errors
import psycopg2.extras
import time
import chardet


import os
import sys
from dotenv import load_dotenv
load_dotenv()

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


def create_table(conn):
    """Create a table named 'transactions'"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE transactions (
                member_id INT,
                member_name VARCHAR(255),
                transaction_type VARCHAR(20),
                created_date DATE,
                price NUMERIC(10, 2)
            )
        """)
        conn.commit()


def drop_table(conn):
    """Drop the 'transactions' table if it exists"""
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS transactions CASCADE;
        """)
        conn.commit()


def table_exists(conn):
    """Check if the 'transactions' table exists"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM   information_schema.tables 
                WHERE  table_name = 'transactions'
            )
        """)
        return cur.fetchone()[0]


def check_partial_load_availability(conn):
    """Check if partial load is available"""
    if not table_exists(conn):
        raise Exception(
            "Partial load is not available. The 'transactions' table does not exist.")


def delete_rows_outside_date_range(conn, start_date, end_date):
    """Delete all rows outside the specified date range"""
    with conn.cursor() as cur:
        cur.execute(sql.SQL("""
            DELETE FROM transactions
            WHERE created_date NOT BETWEEN %s AND %s
        """), (start_date, end_date))
        conn.commit()


def load_parquet_to_postgres(conn, file_path, start_date=None, end_date=None):
    """Load data from a parquet file into a Postgres database"""
    # Load the parquet file into a Pandas DataFrame
    table = pq.read_table(file_path)
    df = table.to_pandas()

    # Connect to the database
    with conn.cursor() as cur:
        # Drop the 'transactions' table if '-f' is used
        if start_date is None and end_date is None:
            drop_table(conn)
            create_table(conn)

        # Check if the 'transactions' table exists if '-p' is used
        else:
            check_partial_load_availability(conn)

        # Delete all rows outside the specified date range if '-p' is used
        if start_date is not None and end_date is not None:
            delete_rows_outside_date_range(conn, start_date, end_date)

        # Load the data into the database
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO transactions (member_id, member_name, transaction_type, created_date, price)
            VALUES (%(member_id)s, %(member_name)s, %(transaction_type)s, %(created_date)s, %(price)s)
        """, df.to_dict('records'))
        conn.commit()


def load_parquet_to_postgres_optimized2(conn, file_path, start_date=None, end_date=None):
    """Load data from a parquet file into a Postgres database"""
    # Load the parquet file into a Pandas DataFrame
    table = pq.read_table(file_path)
    df = table.to_pandas()

    # Define the SQL query
    sql = """
    INSERT INTO transactions (member_id, member_name, transaction_type, created_date, price)
    VALUES ($1, $2, $3, $4, $5)
    """

    # Use context managers for the database connection and cursor
    with conn, conn.cursor() as cur:
        # Drop the 'transactions' table if '-f' is used
        if start_date is None and end_date is None:
            drop_table(conn)
            create_table(conn)

        # Check if the 'transactions' table exists if '-p' is used
        else:
            check_partial_load_availability(conn)

        # Delete all rows outside the specified date range if '-p' is used
        if start_date is not None and end_date is not None:
            delete_rows_outside_date_range(conn, start_date, end_date)

        # Use a prepared statement for the insert
        cur.execute("PREPARE transaction_insert AS {}".format(sql))

        # Use a StringIO object to create a file-like object for the data
        data = io.StringIO(
            df.to_csv(index=False, header=False, sep="\t", na_rep="\\N"))

        # Use the copy_from method to copy data from the file-like object
        cur.copy_from(data, "transactions", sep="\t", null="\\N")

        # Commit the transaction
        conn.commit()


def load_parquet_to_postgres_copy_from(conn, file_path, start_date=None, end_date=None, chunksize=100000):
    """Load data from a parquet file into a Postgres database"""
    # Connect to the database
    with conn.cursor() as cur:
        # Drop the 'transactions' table if '-f' is used
        if start_date is None and end_date is None:
            drop_table(conn)
            create_table(conn)

        # Check if the 'transactions' table exists if '-p' is used
        else:
            check_partial_load_availability(conn)

        # Use COPY command to load the data in bulk
        with open(file_path, 'r') as f:
            with conn.cursor() as cur:
                cur.copy_from(f, 'transactions', sep=',', columns=(
                    'member_id', 'member_name', 'transaction_type', 'created_date', 'price'))

        # Delete all rows outside the specified date range if '-p' is used
        if start_date is not None and end_date is not None:
            delete_rows_outside_date_range(conn, start_date, end_date)

        conn.commit()


def create_view(conn):
    # create cursor
    cur = conn.cursor()

    # create view SQL statement
    view_sql = """
    CREATE OR REPLACE VIEW member_month_summary AS
    SELECT
    member_id,
    DATE_TRUNC('month', created_date) AS month,
    SUM(CASE WHEN transaction_type = 'Purchase' THEN price ELSE 0 END) AS total_purchases,
    SUM(CASE WHEN transaction_type = 'Authorization_only' THEN price ELSE 0 END) AS total_authorizations,
    SUM(CASE WHEN transaction_type = 'Capture' THEN price ELSE 0 END) AS total_captures,
    SUM(CASE WHEN transaction_type = 'Void' THEN price ELSE 0 END) AS total_voids,
    SUM(CASE WHEN transaction_type = 'Refund' THEN price ELSE 0 END) AS total_refunds,
    SUM(CASE WHEN transaction_type = 'Verify' THEN price ELSE 0 END) AS total_verifies
    FROM transactions
    WHERE price > 100
    GROUP BY member_id, month
    ORDER BY member_id, month;
    """

    # execute view creation SQL statement
    cur.execute(view_sql)

    # commit changes
    conn.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Load a parquet file into a Postgres database')
    parser.add_argument('-f', '--full-load', action='store_true',
                        help='Load the full parquet file into the database')
    parser.add_argument('-p', '--partial-load', type=str,
                        help='Load a partial range of the parquet file into the database in the format of "start-date-end-date"')
    parser.add_argument('file', type=str, help='The path to the parquet file')

    args = parser.parse_args()
    print(args)
    start_time = time.time()  # get current time

    conn = connect(conn_params_dic)
    conn.autocommit = True

    # conn = psycopg2.connect(conn_params_dic)

    if args.full_load:
        drop_table(conn)
        create_table(conn)
        load_parquet_to_postgres_optimized2(conn, args.file)
        print("Done loading data by full load ....")

    elif args.partial_load:
        date_range = args.partial_load.split('to')
        print(date_range)
        if len(date_range) != 2:
            print(
                'Invalid date range format. Please use the format of "start-date to end-date" with date format yyyy-mm-dd')
            sys.exit(1)
        start_date, end_date = date_range
        try:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            print(start_date, end_date)
        except ValueError:
            print('Invalid date format. Please use the format of "yyyy-mm-dd"')
            sys.exit(1)

        check_partial_load_availability(conn)
        load_parquet_to_postgres_optimized2(conn, args.file)
        delete_rows_outside_date_range(conn, start_date, end_date)

        print("Done loading data by partial load ....")

    else:
        print('Please specify either "-f" for full load or "-p <start-date>-<end-date>" for partial load.')
        sys.exit(1)

    create_view(conn)
    print("Done creating a view. See Views > member_month_summary")
    end_time = time.time()  # get current time again

    elapsed_time = end_time - start_time
    print(f"Processing took {elapsed_time:.2f} seconds.")

    conn.close()
