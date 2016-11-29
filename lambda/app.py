import urllib2

MESSAGES = {}
API_BASE = 'https://dashboard.cash4code.net/score'
API_TOKEN = 'bb311aeada'

def lambda_handler(msg, context):
    msg_id = msg['Id']  # The unique ID for this message
    part_number = int(msg['PartNumber'])  # Which part of the message it is
    total_parts = int(msg['TotalParts'])  # How many
    data = msg['Data']  # The data of the message

    # Try to get the parts of the message from the MESSAGES dictionary.
    # If it's not there, create one that has None in both parts
    msg = MESSAGES.get(msg_id, dict(sent=False, parts=[None] * total_parts))

    # store this part of the message in the correct part of the list
    msg['parts'][part_number] = data

    # store the parts in MESSAGES
    MESSAGES[msg_id] = msg

    # if both parts are filled, the message is complete
    if None not in msg['parts']:
        if msg['sent']:
            print "not sending again"
            return 'OK'
        print "have all parts"
        # We can build the final message.
        result = ''.join(msg['parts'])
        # sending the response to the score calculator
        # format:
        #   url -> api_base/jFgwN4GvTB1D2QiQsQ8GHwQUbbIJBS6r7ko9RVthXCJqAiobMsLRmsuwZRQTlOEW
        #   headers -> x-gameday-token = API_token
        #   data -> EaXA2G8cVTj1LGuRgv8ZhaGMLpJN2IKBwC5eYzAPNlJwkN4Qu1DIaI3H1zyUdf1H5NITR
        url = API_BASE + '/' + msg_id
        print url
        print result
        req = urllib2.Request(url, data=result, headers={'x-gameday-token':ARGS.API_token})
        resp = urllib2.urlopen(req)
        resp.close()
        msg['sent'] = True
        print resp

    return 'OK'

