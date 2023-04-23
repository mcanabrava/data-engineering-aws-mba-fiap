while True:
    response = sqs.receive_message(QueueUrl='https://us-east-1.console.aws.amazon.com/sqs/v2/home?region=us-east-1#/queues/https%3A%2F%2Fsqs.us-east-1.amazonaws.com%2F785163354234%2Fcsv-to-json', MaxNumberOfMessages=1)
    if 'Messages' in response:
        message = response['Messages'][0]
        body = json.loads(message['Body'])
        receipt_handle = message['ReceiptHandle']

        # Upload the JSON file to the destination bucket
        s3.put_object(Bucket=body['https://s3.console.aws.amazon.com/s3/buckets/rawjson-ecommerce-dataset-fiap-grupo-c?region=us-east-1'], Key=body['key'], Body=body['data'])

        # Delete the message from the SQS queue
        sqs.delete_message(QueueUrl='https://us-east-1.console.aws.amazon.com/sqs/v2/home?region=us-east-1#/queues/https%3A%2F%2Fsqs.us-east-1.amazonaws.com%2F785163354234%2Fcsv-to-json', ReceiptHandle=receipt_handle)
