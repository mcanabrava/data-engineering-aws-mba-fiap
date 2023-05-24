import boto3
import os
import json
import time
import math
from botocore.exceptions import ClientError

# Initialize S3 and SQS clients
s3 = boto3.client('s3')
firehose = boto3.client('firehose')
sqs = boto3.client('sqs')

def create_firehose_records(bucket_name, file_name):
    try:
        # Read the JSON file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        print(f"response: {response}")
        json_content = response['Body'].read().decode('utf-8')
        print(f"json_content: {json_content}")

        # Parse the JSON content
        data = json.loads(json_content)

        # Split the data into smaller chunks
        max_records_per_batch = 100  # Adjust this value as needed
        chunks = [data[i:i + max_records_per_batch] for i in range(0, len(data), max_records_per_batch)]

        # Create a list of record batches to send
        record_batches = []
        for chunk in chunks:
            records = [{'Data': json.dumps(row)} for row in chunk]
            record_batches.append(records)

        # Send record batches to Kinesis Firehose for delivery
        for records in record_batches:
            print(f"Sending record batch: {records}")
            response = firehose.put_record_batch(
                DeliveryStreamName='PUT-S3-ingestion',
                Records=records
            )

            if response['FailedPutCount'] > 0:
                print(f"Some records failed to deliver to Kinesis Firehose for file {file_name}")
            else:
                print(f"Some records delivered successfully for file {file_name}")

        # Handle remaining records (less than max_records_per_batch)
        remaining_records = data[len(record_batches) * max_records_per_batch:]
        if remaining_records:
            records = [{'Data': json.dumps(row)} for row in remaining_records]
            response = firehose.put_record_batch(
                DeliveryStreamName='PUT-S3-ingestion',
                Records=records
            )

            if response['FailedPutCount'] > 0:
                print(f"Some remaining records failed to deliver to Kinesis Firehose for file {file_name}")
            else:
                print(f"All remaining records delivered successfully for file {file_name}")

    except ClientError as e:
        print(f"Error creating Firehose record for file {file_name}: {e}")
        raise

# Retrieve messages from SQS queue
def lambda_handler(event, context):
    
    for record in event['Records']:
        try:
            # Get message body and extract file name
            message_body = json.loads(record['body'])
            file_name = message_body['file_name']
            print(f"Processing file {file_name}")

            # Create Firehose record for the file
            create_firehose_records('raw-json-ecommerce-dataset-fiap-grupo-c', file_name)
            print(f'{file_name} uploaded to the bucket')

            # Delete message from SQS queue
            sqs.delete_message(
                QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/json-to-firehose',
                ReceiptHandle=record['receiptHandle']
            )
        
            print(f'{file_name} was deleted from the queue')
        except Exception as e:
            print(f"Error processing message: {e}")


print("Process finished")
