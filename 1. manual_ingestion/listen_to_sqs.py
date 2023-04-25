import boto3
import json

# Initialize S3 and SQS clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

while True:
    response = sqs.receive_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/785163354234/csv-to-json', MaxNumberOfMessages=1)
    if 'Messages' in response:
        message = response['Messages'][0]
        body = json.loads(message['Body'])
        receipt_handle = message['ReceiptHandle']

        # Upload the JSON file to the destination bucket
        try:
            s3.put_object(Bucket='destination-bucket-name', Key=body['key'], Body=body['data'])
            print(f"Successfully uploaded JSON file {body['key']} to destination bucket.")
        except KeyError:
            print("Error uploading JSON file: missing 'key' key in message body.")
        except Exception as e:
            print(f"Error uploading JSON file {body['key']} to destination bucket: {e}")



        # Delete the message from the SQS queue
        sqs.delete_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/785163354234/csv-to-json', ReceiptHandle=receipt_handle)
