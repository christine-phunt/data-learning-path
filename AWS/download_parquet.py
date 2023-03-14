import boto3
import os

# create S3 client
s3 = boto3.client('s3')

# specify bucket name and file names
bucket_name = 'datalearningchris'
source_file_name = 'output.parquet'

# create directory to store the downloaded files
if not os.path.exists('output'):
    os.makedirs('output')


def download_file(bucket_name, source_file_name):
    print(" *************** ")
    print(f"Donwloading {source_file_name} from s3 bucket - {bucket_name}.")
    print(" *************** ")

    s3.download_file(bucket_name, source_file_name,
                     f'output/{source_file_name}')
    head_response = s3.head_object(
        Bucket=bucket_name, Key=source_file_name)
    if head_response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print(" *************** ")
        print(f"--- {source_file_name} downloaded successfully.")
    else:
        print(" *************** ")
        raise Exception(f"File {source_file_name} download failed.")

    # check if file is downloaded successfully
    if not os.path.isfile(f"output/{source_file_name}"):
        raise FileNotFoundError(f"output/{source_file_name} file not found")

    else:
        print(" *************** ")
        print(f"output/{source_file_name} file exists on the directory.")


download_file(bucket_name, source_file_name)
