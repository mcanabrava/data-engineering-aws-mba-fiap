import boto3
import json

def lambda_handler(event, context):
    # Extract the bucket name and key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    # Initialize S3 and SQS clients
    s3 = boto3.client('s3')
    firehose = boto3.client('firehose')
    sqs = boto3.client('sqs')

    # Check if the uploaded file is a JSON
    if file_key.endswith('.json'):
        # Send message to SQS queue
        message = {
            'file_name': file_key
        }
        try:
            response = sqs.send_message(
                QueueUrl='https://sqs.us-east-2.amazonaws.com/785163354234/json-to-firehose',
                MessageBody=json.dumps(message)
            )
            print(f"Sent message to SQS queue for file {file_key}")
        except Exception as e:
            print(f"Error sending message to SQS queue for file {file_key}: {e}")

    print("All Messages Processed!")
