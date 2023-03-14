import boto3
import optimized_ingest
import os

# create S3 client
s3 = boto3.client('s3')

# specify bucket name and file names
bucket_name = 'datalearningchris'
source_file_names = ['member_profile.csv', 'generated_transactions.csv']
dest_file_name = 'output.parquet'

try:
    # download the files from S3
    download_success = True
    file_present = False

    # create directory to store the downloaded files
    if not os.path.exists('data'):
        os.makedirs('data')

    print(" *************** ")
    print(" *************** ")
    print(" *************** ")
    print(f"Downloading files from s3.")

    for source_file_name in source_file_names:
        print(" . . . . .")
        if download_success:
            s3.download_file(bucket_name, source_file_name,
                             f'data/{source_file_name}')
            head_response = s3.head_object(
                Bucket=bucket_name, Key=source_file_name)
            if head_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"--- {source_file_name} downloaded successfully.")
            else:
                download_success = False
                print(" *************** ")
                raise Exception(f"File {source_file_name} download failed.")

            # check if file is downloaded successfully
            with open(f"data/{source_file_name}", 'r') as f:
                if not f.readline():
                    download_success = False
                    print(" *************** ")
                    raise Exception(f"{source_file_name} is empty.")
                else:
                    file_present = True

    if (download_success and file_present):
        # Call optimized_ingest file to convert to parquet
        optimized_ingest.ingest()
        print(" *************** ")
        print("Files successfully converted to parquet.")

        if not os.path.isfile(dest_file_name):
            raise FileNotFoundError(f"{dest_file_name} file not found")

        else:
            print(" *************** ")
            print(f"{dest_file_name} file found.")

        # upload the file to S3
        print(" *************** ")
        print(" *************** ")
        print(" *************** ")
        print(f"{dest_file_name} file now uploading to s3. . . .")
        s3.upload_file(f"data/{dest_file_name}", bucket_name, dest_file_name)
        head_response = s3.head_object(Bucket=bucket_name, Key=dest_file_name)
        if head_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(" *************** ")
            print(f"File uploaded to s3 bucket {bucket_name} successfully.")
        else:
            print(" *************** ")
            raise Exception("File upload failed.")

except Exception as e:
    print(" *************** ")
    print("Error:", e)

finally:
    print(" *************** ")
    print("Done ingest process.")
