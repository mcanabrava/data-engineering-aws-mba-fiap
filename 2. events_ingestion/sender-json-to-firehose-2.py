import boto3
import os
import json
import csv

def lambda_handler(event, context):
    # Initialize S3 and SQS clients
    s3 = boto3.client('s3')
    sqs = boto3.client('sqs')

    for record in event['Records']:
        try:
            # Get message body and extract file name
            message_body = json.loads(record['body'])
            file_name = message_body['file_name']
            print(f"Processing file {file_name}")

            # Check if file exists in destination bucket
            if check_file_exists('raw-json-ecommerce-dataset-fiap-grupo-c', file_name.replace('.csv', '.json')):
                print(f'File {file_name} already exists in the destination bucket')
            else:
                print(f"{file_name} does not exists in json bucket yet, let's create it!")
                # Download CSV file from S3
                csv_obj = s3.get_object(Bucket='raw-csv-ecommerce-dataset-fiap-grupo-c', Key=file_name)
                print(f"{file_name} retrieved from bucket")
                csv_data = csv_obj['Body'].read().decode('utf-8')

                # Convert CSV to JSON
                csv_reader = csv.DictReader(csv_data.splitlines())
                json_data = json.dumps(list(csv_reader))
                print(f"{file_name} converted to JSON")

                # Upload JSON file to S3
                s3.put_object(Bucket='raw-json-ecommerce-dataset-fiap-grupo-c', Key=file_name.replace('.csv', '.json'), Body=json_data)
                print(f"{file_name} uploaded in JSON format")

                # Delete message from SQS queue
                sqs.delete_message(
                    QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/csv-to-json',
                    ReceiptHandle=record['receiptHandle']
                )
                print(f"{file_name} message deleted")

                # Print success message
                print(f"Successfully completed the process for {file_name}")
        except Exception as e:
            # Print error message
            print(f"Error processing {file_name}: {e}")

            # Delete message from SQS queue
            sqs.delete_message(
                QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/csv-to-json',
                ReceiptHandle=record['receiptHandle']
            )

    print("All Files Uploaded!")

def check_file_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False
