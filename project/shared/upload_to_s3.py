import boto3
import sys
from .read_yaml import read_yaml

#Function to Upload to s3 bucket
def uplaod_to_s3(file_path,bucket_name,object_key):
    secrets = read_yaml("/app/project/configs/secrets.yaml")
    access_key = secrets["aws"]["aws_access_key_id"]
    secret_key = secrets["aws"]["aws_secret_access_key"]
    # Create an S3 client
    s3_client = boto3.client('s3',
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_key,
                            )
    # Upload the file
    try:
        s3_client.upload_file(file_path,bucket_name,object_key)
        print(f"File uploaded successfully to s3://{bucket_name}/{object_key}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        sys.exit(1)