import boto3
import os
import json
import csv
import time


# Define function to check if file exists in S3 bucket
def check_file_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False

# Initialize S3 and SQS clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

# List all objects in the source S3 bucket
response = s3.list_objects_v2(Bucket='raw-csv-ecommerce-dataset-fiap-grupo-c')


# Send SQS messages for all the objects in the bucket
for obj in response['Contents']:
    # Check if object is a CSV file
    if obj['Key'].endswith('.csv'):
        # Send message to SQS queue
        message = {
            'file_name': obj['Key']
        }
        try:
            response = sqs.send_message(QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/csv-to-json', MessageBody=json.dumps(message))
            print(f"Sent message to SQS queue for file {obj['Key']}")
        except Exception as e:
            print(f"Error sending message to SQS queue for file {obj['Key']}: {e}")
            

# Wait for 5 seconds to ensure messages have been delivered to SQS
print("Preparing to retrieve messages...")
time.sleep(3)


response = sqs.receive_message(QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/csv-to-json', MaxNumberOfMessages=10)

# Printing existing messages
for message in response.get('Messages', []):
    file_name = json.loads(message['Body'])['file_name']
    print(f"Found message for: {file_name}")
            
# Process messages and deliver them to the destination S3 bucket via Kinesis Firehose
for message in response.get('Messages', []):
    try:
        # Get file name from message
        file_name = json.loads(message['Body'])['file_name']
        
        # Check if file exists in destination bucket
        if check_file_exists('raw-json-ecommerce-dataset-fiap-grupo-c', file_name):
            print(f'File {file_name} already exists in destination bucket')
        else:
            # Send file to Kinesis Firehose for delivery to destination S3 bucket
            delivery_stream = 'json-to-s3-firehose'
            record = {'Data': f's3://raw-json-ecommerce-dataset-fiap-grupo-c/{file_name}'}
            firehose = boto3.client('firehose')
            response = firehose.put_record(DeliveryStreamName=delivery_stream, Record=record)
            print(f"Sent file {file_name} to Kinesis Firehose for delivery")
        
        # Delete message from SQS queue
        sqs.delete_message(QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/json-to-firehose', ReceiptHandle=message['ReceiptHandle'])

    except Exception as e:
        print(f"Error processing message: {e}")

# Flush records from Kinesis Firehose
try:
    response = firehose.flush_delivery_stream(DeliveryStreamName='json-to-s3-firehose')
    print(f"Flushed records from Kinesis Firehose")
except Exception as e:
    print(f"Error flushing records from Kinesis Firehose: {e}")

print("Process completed")