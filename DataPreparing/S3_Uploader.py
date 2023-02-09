#################################### IMPORTING LIBRARIES #####################################

import boto3
import os
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
load_dotenv()

#################################### CONFIGURATIONS ##########################################

ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')
bucket_name = os.environ.get('BUCKET_NAME')
S3_FILE_PATH = "PreparedData/"

#################################### UPLOADING TO S3 BUCKET ####################################


def upload_generated_csv_s3(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    try:
        s3.upload_file(local_file, bucket, s3_file)
        return True
    except FileNotFoundError:
        return False
    except NoCredentialsError:
        return False
      
try:
    # uploading Prepared Dataset into S3 bucket
    def Trigger_Uploader(file_path,file_name):
        if ACCESS_KEY is not None:
            uploaded = upload_generated_csv_s3(local_file=os.path.join(file_path, file_name),
                                                  bucket=bucket_name,
                                                  s3_file=f"{file_path}{file_name}")
            if uploaded == True:
                return "Uploaded to S3 bucket"
        else:
            raise Exception("Keys are missing or not reading from .env")
except Exception as e:
    print(e)