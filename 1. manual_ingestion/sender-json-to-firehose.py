import boto3
import os
import json
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
firehose = boto3.client('firehose')
sqs = boto3.client('sqs')

# List all objects in the source S3 bucket
response = s3.list_objects_v2(Bucket='raw-json-ecommerce-dataset-fiap-grupo-c')
files = response.get('Contents', [])

for file in response.get('Contents', []):
    print(f"We have found the following file {file['Key']} in the bucket")

# Send SQS messages for all the objects in the bucket
for obj in files:
    # Check if object is a JSON file
    if obj['Key'].endswith('.json'):
        # Send message to SQS queue
        message = {
            'file_name': obj['Key']
        }
        try:
            response = sqs.send_message(QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/json-to-firehose', MessageBody=json.dumps(message))
            print(f"Sent message to SQS queue for file {obj['Key']}")
        except Exception as e:
            print(f"Error sending message to SQS queue for file {obj['Key']}: {e}")

# Wait seconds to ensure records have been delivered to SQS
print("Preparing to retrieve messages in 10 seconds")
time.sleep(10)
print("Retrieving messages...")


# Retrieve messages from SQS queue
response = sqs.receive_message(QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/json-to-firehose', MaxNumberOfMessages=10,
    VisibilityTimeout=50,
    WaitTimeSeconds=20,
    MessageAttributeNames=['All'])

print("We have identified the following messages in the SQS:")
for message in response.get('Messages', []):
    print(message['Body'])


# Process messages and deliver them to the destination S3 bucket via Kinesis Firehose
for message in response.get('Messages', []):
    try:
        # Get file name from message
        file_name = json.loads(message['Body'])['file_name']
        
        # Check if file exists in destination bucket
        if check_file_exists('ingested-json-ecommerce-dataset-fiap-grupo-c', file_name):
            print(f'File {file_name} already exists in destination bucket')
        else:
            # Send file to Kinesis Firehose for delivery to destination S3 bucket
            delivery_stream = 'PUT-S3-ingestion'
            record = {'Data': f's3://raw-json-ecommerce-dataset-fiap-grupo-c/{file_name}'}
            firehose = boto3.client('firehose')
            response = firehose.put_record(DeliveryStreamName=delivery_stream, Record=record)
            print(f"Sent file {file_name} to Kinesis Firehose for delivery")
        
        # Delete message from SQS queue
        sqs.delete_message(QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/json-to-firehose', ReceiptHandle=message['ReceiptHandle'])

    except Exception as e:
        print(f"Error processing message: {e}")
