from boto3.session import Session
import subprocess
import os


ACCESS_KEY = os.environ.get('P_AWS_ACCESS_KEY')
SECRET_KEY = os.environ.get('P_AWS_SECRET_KEY')
BUCKET_NAME = os.environ.get('P_AWS_BUCKET_NAME')

session = Session(aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY)

S3directory = 'invoice_import' 
S3filename = 'invoices.txt'
s3Path=f"{S3directory}/{S3filename}"
Localdirectory = '/Users/igale/Downloads'
localPath = f"{Localdirectory}/{S3filename}"

s3 = session.resource('s3').Bucket(BUCKET_NAME).download_file(s3Path, localPath)

###----------------------- SSH Transfer file to remote
host =  os.environ.get('P_REMOTE_HOST')
port = os.environ.get('P_REMOTE_PORT')
username = os.environ.get('P_REMOTE_USERNAME')
ftpDirectory = os.environ.get('P_REMOTE_PATH')

ftpPath = f"{username}@{host}:/{ftpDirectory}/{S3filename}"

# SCP to remote server
subprocess.run(['scp', localPath, ftpPath])

## --------------- List all Bucket objects:
for obj in s3.objects.all():
    print(obj)

## --------------- List Bucket objects by filter:
for b in s3.objects.filter(Prefix='incoming/priority'):
    print(obj)
