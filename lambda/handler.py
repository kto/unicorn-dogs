#!/usr/bin/env python
"""
Client which receives the requests

Args:
    API Token
    API Base (https://...)

"""
import urllib2
import redis
import os
import json

#logging.basicConfig(level=logging.DEBUG)

# defining global vars

CACHE_ENDPOINT = os.environ.get('REDIS_ENDPOINT', 'unicorn-dogs.nmwbnf.ng.0001.euc1.cache.amazonaws.com')


class Handler(object):


    def __init__(self, api_token, api_base, logger):
        self.logger = logger
        self.redis_client = None
        self.api_token = api_token
        self.api_base = api_base

        if CACHE_ENDPOINT is not None:
            self.redis_client = redis.StrictRedis(host=CACHE_ENDPOINT, port=6379, db=0)

    def process_message(self, data):
        """
        processes the messages by combining and appending the kind code
        """
        msg_id = data['Id']  # The unique ID for this message
        part_number = int(data['PartNumber'])  # Which part of the message it is
        total_parts = int(data['TotalParts'])  # How many
        raw = data['Data']  # The data of the message

        print "received", msg_id, part_number, raw
        # Try to get the parts of the message from the MESSAGES dictionary.
        # If it's not there, create one that has None in both parts
        msg = self.redis_client.get(msg_id)
        if msg:
            msg = json.loads(msg)
        else:
            msg = dict(sent=False, parts=[None] * total_parts)

        # store this part of the message in the correct part of the list
        msg['parts'][part_number] = raw

        # store the parts in MESSAGES
        self.redis_client.set(msg_id, json.dumps(msg))

        # if both parts are filled, the message is complete
        if None not in msg['parts']:
            if msg['sent']:
                print "not sending again"
                return 'OK'
            # app.logger.debug("got a complete message for %s" % msg_id)
            print "have all parts, sending response to", self.api_base
            # We can build the final message.
            result = ''.join(msg['parts'])
            # sending the response to the score calculator
            # format:
            #   url -> api_base/jFgwN4GvTB1D2QiQsQ8GHwQUbbIJBS6r7ko9RVthXCJqAiobMsLRmsuwZRQTlOEW
            #   headers -> x-gameday-token = API_token
            #   data -> EaXA2G8cVTj1LGuRgv8ZhaGMLpJN2IKBwC5eYzAPNlJwkN4Qu1DIaI3H1zyUdf1H5NITR
            self.logger.debug("ID: %s" % msg_id)
            self.logger.debug("RESULT: %s" % result)
            url = self.api_base + '/' + msg_id
            req = urllib2.Request(url, data=result, headers={'x-gameday-token': self.api_token})
            resp = urllib2.urlopen(req)
            resp.close()
            msg['sent'] = True
            self.redis_client.set(msg_id, json.dumps(msg))

        return 'OK'
