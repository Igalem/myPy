import codecs
from boto3.session import Session
import subprocess
import os

ACCESS_KEY = os.environ.get('P_AWS_ACCESS_KEY')
SECRET_KEY = os.environ.get('P_AWS_SECRET_KEY')
BUCKET_NAME = os.environ.get('P_AWS_BUCKET_NAME')

BLOCKSIZE = os.environ.get('P_CONTENT_BLOCKSIZE')  ## 1048576 or some other #, desired size in bytes

DECODE = 'utf-8'
ENCODE = 'utf-16'

## Source Key
s3Directory = 'xxxxx' 
s3FilenamePrefix = 'xxxxx'
s3FilenameTemp = 'xxxxx'
s3FilenameSufix = 'xxxxx'
s3Filename = f"{s3FilenamePrefix}.{s3FilenameSufix}"
s3Path=f"{s3Directory}/{s3Filename}"

## Local file
Localdirectory = 'xxxxx'
sourceFileName = f"{Localdirectory}/{s3Filename}"


SESSION = Session(aws_access_key_id=ACCESS_KEY,
              aws_secret_access_key=SECRET_KEY)

s3 = SESSION.resource('s3')

SKAIBILL_ENTITIES = ['UK', 'JAP', 'GER', 'USA']

for entity in SKAIBILL_ENTITIES:
    s3FilenamePrefix = s3FilenameTemp.replace('XXXX',  entity)
    s3Filename = f"{s3FilenamePrefix}.{s3FilenameSufix}"
    s3Path=f"{s3Directory}/{s3Filename}"
    sourceFileName = f"{Localdirectory}/{s3Filename}"
    # object = s3.meta.client.get_object(Bucket=BUCKET_NAME, Key=s3Path)

    ## Download sourceFile:
    s3.Bucket(BUCKET_NAME).download_file(s3Path, sourceFileName)

    ## Decode source local file to target file UTF16-LE
    targetFileName = f"{s3FilenamePrefix}_{ENCODE}.{s3FilenameSufix}"
    targetFilePath = f"{Localdirectory}/{targetFileName}"

    # with codecs.readbuffer_encode(dataBytes) as sourceFile:
    with codecs.open(sourceFileName, "r", f"{DECODE}") as sourceFile:
        with codecs.open(targetFilePath, "w", f"{ENCODE}") as targetFile:
            while True:
                contents = sourceFile.read(BLOCKSIZE)
                if not contents:
                    break
                targetFile.write(contents)

    # ## Upload target file as AWS Key into Bucket:
    # s3.Bucket(BUCKET_NAME).upload_file(targetFilePath, f"{s3Directory}/{targetFileName}")


