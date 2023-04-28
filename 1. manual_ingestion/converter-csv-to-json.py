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
time.sleep(5)

# Printing existing messages
for message in response.get('Messages', []):
    file_name = json.loads(message['Body'])['file_name']
    print(f"Found message for: {file_name}")
            
while True:
    # Receive messages from SQS
    response = sqs.receive_message(
        QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/csv-to-json',
        MaxNumberOfMessages=1
    )

    # Check if there are no more messages
    if 'Messages' not in response:
        break

    message = response['Messages'][0]
    
    
    try:
        # Get file name from message
        file_name = json.loads(message['Body'])['file_name']
        print(f"found message for file {file_name}")
        
        # Check if file exists in destination bucket
        if check_file_exists('raw-json-ecommerce-dataset-fiap-grupo-c', file_name.replace('.csv', '.json')):
            print(f'File {file_name} already exists in the destination bucket')
        else:
            # Download CSV file from S3
            csv_obj = s3.get_object(Bucket='raw-csv-ecommerce-dataset-fiap-grupo-c', Key=file_name)
            csv_data = csv_obj['Body'].read().decode('utf-8')
    
            # Convert CSV to JSON
            csv_reader = csv.DictReader(csv_data.splitlines())
            json_data = json.dumps(list(csv_reader))
    
            # Upload JSON file to S3
            s3.put_object(Bucket='raw-json-ecommerce-dataset-fiap-grupo-c', Key=file_name.replace('.csv', '.json'), Body=json_data)
            
            # Delete message from SQS queue
            sqs.delete_message(
                QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/csv-to-json',
                ReceiptHandle=message['ReceiptHandle']
            )
    
            # Print success message
            print(f"Successfully uploaded {file_name} in JSON format")
    except Exception as e:
        # Print error message
        print(f"Error processing {file_name}: {e}")
        
        # Delete message from SQS queue
        sqs.delete_message(
            QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/csv-to-json',
            ReceiptHandle=message['ReceiptHandle']
        )


print("Process completed")