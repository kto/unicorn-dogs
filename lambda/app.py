import urllib2
import boto3
import json
import logging
import handler

API_BASE = 'https://dashboard.cash4code.net/score'
API_TOKEN = 'bb311aeada'
handler = Handler(API_TOKEN, API_BASE, logging.getLogger())

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        msg_data = s3_client.get_object(Bucket=bucket, Key=key)
        if 'Body' in msg_data:
            return handler.process_message(json.load(msg_data['Body']))

