import boto3
import csv
import json
import os

# List all objects in the source S3 bucket
response = s3.list_objects_v2(Bucket=raw-ecommerce-dataset-fiap-grupo-c)

# Loop through all objects in the source bucket
for obj in response['Contents']:
    # Check if object is a CSV file
    if obj['Key'].endswith('.csv'):
        # Download CSV file from S3
        csv_obj = s3.get_object(Bucket=raw-ecommerce-dataset-fiap-grupo-c, Key=obj['Key'])
        csv_data = csv_obj['Body'].read().decode('utf-8')

        # Convert CSV to JSON
        csv_reader = csv.DictReader(csv_data.splitlines())
        json_data = json.dumps(list(csv_reader))

        # Send message to SQS queue
        message = {
            'destination_bucket': rawjson-ecommerce-dataset-fiap-grupo-c,
            'destination_key': obj['Key'].replace('.csv', '.json')
        }
        response = sqs.send_message(QueueUrl='https://us-east-1.console.aws.amazon.com/sqs/v2/home?region=us-east-1#/queues/https%3A%2F%2Fsqs.us-east-1.amazonaws.com%2F785163354234%2Fcsv-to-json', MessageBody=json.dumps(message))
        print(f"Sent message to SQS queue for file {obj['Key']}")
