import boto3
import psycopg2
import csv
import os

def lambda_handler(event, context):
    # PostgreSQL connection details
    db_host = '<YOUR_DB_HOST>'
    db_port = '<YOUR_DB_PORT>'
    db_name = '<YOUR_DB_NAME>'
    db_user = '<YOUR_DB_USERNAME>'
    db_password = '<YOUR_DB_PASSWORD>'

    # S3 bucket details
    s3_bucket_name = '<YOUR_S3_BUCKET_NAME>'
    s3_folder_path = '<YOUR_S3_FOLDER_PATH>'

    # PostgreSQL query to retrieve data
    query = '<YOUR_POSTGRES_QUERY>'

    # Establish connection to PostgreSQL
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )

    # Create a cursor to execute the query
    cursor = conn.cursor()

    # Execute the PostgreSQL query
    cursor.execute(query)

    # Fetch all rows from the result
    rows = cursor.fetchall()

    # Generate a unique filename for the CSV file
    csv_filename = 'data.csv'
    
    # Define the local file path for the CSV file
    local_file_path = f'/tmp/{csv_filename}'
    
    # Write the data to a CSV file
    with open(local_file_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(rows)

    # Upload the CSV file to S3
    s3 = boto3.client('s3')
    s3_key = f'{s3_folder_path}/{csv_filename}'
    s3.upload_file(local_file_path, s3_bucket_name, s3_key)

    # Clean up the local CSV file
    os.remove(local_file_path)

    # Close the cursor and database connection
    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': 'Data imported successfully to S3'
    }
