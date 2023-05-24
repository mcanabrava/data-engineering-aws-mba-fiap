## The purpose of this file is to guide the manual ingestion process

1. Run the script that sends messages to the SQS queue and convert the files from CSV to JSON, adding them to the json bucket: python converter-csv-to-json.py
2. Run the script that listens to the SQS queue and to get files from the JSON bucket and upload them to the destination Firehose bucket: python sender-json-to-firehose.py
