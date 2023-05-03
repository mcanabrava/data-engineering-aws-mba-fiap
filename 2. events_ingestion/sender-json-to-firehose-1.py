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


print ("All Messsages Processed!")