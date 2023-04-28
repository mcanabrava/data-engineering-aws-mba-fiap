import boto3
import os
import json
import time
import math
from botocore.exceptions import ClientError

# Define function to check if file exists in S3 bucket
import boto3

def check_file_exists(bucket, key):
    s3 = boto3.client('s3')
    
    # Recursive function to search for the file in the bucket
    def search_file(prefix):
        objects = s3.list_objects_v2(Bucket='ingested-json-ecommerce-dataset-fiap-grupo-c', Prefix=prefix)
        for obj in objects.get('Contents', []):
            if obj['Key'] == key:
                return True
            if obj['Key'].endswith('/'):
                # Recursively search within the nested folder
                if search_file(obj['Key']):
                    return True
        return False
    
    # Start the recursive search from the root folder
    return search_file('')


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
print("Preparing to retrieve messages in 5 seconds")
time.sleep(5)
print("Retrieving messages...")

def split_json_into_chunks(json_content, max_chunk_size):
    # Calculate the number of chunks based on the desired maximum chunk size
    num_chunks = int(math.ceil(len(json_content) / max_chunk_size))
    
    # Split the JSON content into chunks
    chunks = []
    for i in range(num_chunks):
        start = i * max_chunk_size
        end = start + max_chunk_size
        chunk = json_content[start:end]
        chunks.append(chunk)
    
    return chunks


def create_firehose_records(bucket_name, file_name):
    try:
        # Read the JSON file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        json_content = response['Body'].read().decode('utf-8')

        # Split JSON content into chunks
        max_chunk_size = 3 * 1024  # Maximum chunk size of 3 MB
        chunks = split_json_into_chunks(json_content, max_chunk_size)
        
        # Create a list of records to send
        records = [{'Data': chunk} for chunk in chunks]

        # Send JSON records to Kinesis Firehose for delivery
        response = firehose.put_record_batch(
            DeliveryStreamName='PUT-S3-ingestion',
            Records=records
        )
        
        if response['FailedPutCount'] > 0:
            print(f"Some records failed to deliver to Kinesis Firehose for file {file_name}")
        else:
            print(f"All records delivered successfully for file {file_name}")
        
    except ClientError as e:
        print(f"Error creating Firehose record for file {file_name}: {e}")
        raise

# Retrieve messages from SQS queue
while True:
    response = sqs.receive_message(
        QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/json-to-firehose',
        MaxNumberOfMessages=10,
        VisibilityTimeout=50,
        WaitTimeSeconds=20,
        MessageAttributeNames=['All']
    )

    messages = response.get('Messages', [])
    if not messages:
        break

    print("We have identified the following messages in the SQS:")
    for message in messages:
        print(message['Body'])

    # Process messages and deliver them to the destination S3 bucket via Kinesis Firehose
    for message in messages:
        try:
            # Get file name from message
            file_name = json.loads(message['Body'])['file_name']

            # Check if file exists in source bucket
            if not check_file_exists('ingested-json-ecommerce-dataset-fiap-grupo-c', file_name):
                print(f'File {file_name} does not exist in destionation bucket')

                # Create Firehose record for the file
                create_firehose_records('raw-json-ecommerce-dataset-fiap-grupo-c', file_name)
                print(f'{file_name} uploaded to the bucket')
    
                # Delete message from SQS queue
                sqs.delete_message(
                    QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/json-to-firehose',
                    ReceiptHandle=message['ReceiptHandle']
                )
            
            print(f'{file_name} already exists in destionation bucket')
        except Exception as e:
            print(f"Error processing message: {e}")


print(f"Process finished")