#Ingest and Load to DB Script 

##Overview
This repository contains two scripts: ingest.py and load_to_db.py.

ingest.py converts a csv file to a parquet file.
load_to_db.py loads the parquet file to the database and creates a View that shows a summary report per member per month.

##How to Run
Run the ingest.py script by executing the command python3 ingest.py.
Run the load_to_db.py script using one of the following commands:
For a full load: python3 load_to_db.py -f output.parquet
For a partial load between specific dates: python3 load_to_db.py -p 2022-01-01to2022-05-30 output.parquet

##Dependencies
Python 3.6 or above
Pandas library
Pyarrow library
PostgreSQL database

##Configuration
Edit the .env file with your database connection details.
