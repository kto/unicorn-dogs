import base64
import logging
import handler
import json

API_BASE = 'https://dashboard.cash4code.net/score'
API_TOKEN = 'bb311aeada'
handler = handler.Handler(API_TOKEN, API_BASE, logging.getLogger())


def lambda_handler(event, context):
    for record in event['Records']:
       #Kinesis data is base64 encoded so decode here
       payload=base64.b64decode(record["kinesis"]["data"])
       return handler.process_message(json.loads(payload))


