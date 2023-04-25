import boto3
import csv
import json
import os

# Initialize S3 and SQS clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

# List all objects in the source S3 bucket
response = s3.list_objects_v2(Bucket='raw-csv-ecommerce-dataset-fiap-grupo-c')

# Loop through all objects in the source bucket
for obj in response['Contents']:
    # Check if object is a CSV file
    if obj['Key'].endswith('.csv'):
        # Download CSV file from S3
        csv_obj = s3.get_object(Bucket='raw-csv-ecommerce-dataset-fiap-grupo-c', Key=obj['Key'])
        csv_data = csv_obj['Body'].read().decode('utf-8')

        # Convert CSV to JSON
        csv_reader = csv.DictReader(csv_data.splitlines())
        json_data = json.dumps(list(csv_reader))

        # Send message to SQS queue
        message = {
            'destination_bucket': 'raw-json-ecommerce-dataset-fiap-grupo-c',
            'destination_key': obj['Key'].replace('.csv', '.json')
        }
        try:
            response = sqs.send_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/785163354234/csv-to-json', MessageBody=json.dumps(message))
            print(f"Sent message to SQS queue for file {obj['Key']}")
        except Exception as e:
            print(f"Error sending message to SQS queue for file {obj['Key']}: {e}")