# Ingest and Load to DB Script 

## Overview
This repository contains scripts for creating a bucket in s3, download file from s3, converting the csv file to parquet, uploading the file to s3, then loading he parquet file to the database and creates a View

- `set-aws-credentials.sh` a script that sets aws credentials
- `create_bucket.py` - Creates a bucket in s3
- `download_from_s3.py` downloads the 'member_profile.csv', 'generated_transactions.csv' from the s3 bucket named 'datalearningchris' and call the optimized_ingest inside to process the file and convert it to parquet then upload it again o s3
- `download_parquet_from_s3.py` download the 'output.parquet' from s3 
- `optimized_ingest.py` converts a csv file to a parquet file.
- `load_to_db.py loads` the parquet file to the database and creates a View that shows a summary report per member per month.

## How to Run
1. Run `chmod +x set-aws-credentials.sh` Make sure to make the script executable
2. Copy the credentials from AWS 
3. Run the `./set-aws-credentials.sh` to set the aws credentials
4. Make sure the 'datalearningchris' bcuket in s3 is created already with files 'member_profile.csv', 'generated_transactions.csv' in it.
5. Run the `download_from_s3.py` by executing the command `python3 download_from_s3.py` to
6. Run the `download_parquet_from_s3.py` by executing the command `python3 download_parquet_from_s3.py` to
7. Run the load_to_db.py script using one of the following commands:
```
For a full load: python3 load_to_db.py -f output/output.parquet
For a partial load between specific dates: python3 load_to_db.py -p 2022-01-01to2022-05-30 output/output.parquet
```

## Dependencies
- Python 3.6 or above
- Pandas library
- Pyarrow library
- PostgreSQL database

## Configuration
- Edit the .env file with your database connection details.
- Edit the AWS.env file with your AWS connection details.
