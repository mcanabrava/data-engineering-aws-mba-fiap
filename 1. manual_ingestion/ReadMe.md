## The purpose of this file is to guide the manual ingestion process

1. Run the script that sends messages to the SQS queue: python converter-csv-to-json.py
2. Run the script that listens to the SQS queue and uploads the files to the destination bucket: python sender-json-to-firehose.py
