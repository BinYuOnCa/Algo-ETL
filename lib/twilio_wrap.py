

import requests

import util.error as error
from util.config import global_config
import secrets


def send_sms(msg):
    url = 'https://api.twilio.com/2010-04-01/Accounts/ACe164076dba47b1cfc8a02e16b780c42a/Messages.json'
    data = {'To': global_config['twilio']['to'], 'From': global_config['twilio']['from'], 'Body': msg}
    response = requests.post(
        url, data=data,
        auth=(secrets.TWILIO_USER, secrets.TWILIO_PASS)
    )
    if response.status_code != 201:
        raise error.LocalError(f'Fail to send sms {msg}. Response:{response.status_code}')


'''
curl 'https://api.twilio.com/2010-04-01/Accounts/ACe164076dba47b1cfc8a02e16b780c42a/Messages.json' -X POST \
--data-urlencode 'To=+19052087755' \
--data-urlencode 'From=+16182128108' \
--data-urlencode 'Body=hello from twilio' \
-u ACe164076dba47b1cfc8a02e16b780c42a:[AuthToken]

Response
{
    "sid": "SMc070607afd0d4f668b0e36ede9acebab",
    "date_created": "Tue, 12 Jan 2021 18:49:01 +0000",
    "date_updated": "Tue, 12 Jan 2021 18:49:01 +0000",
    "date_sent": null,
    "account_sid": "ACe164076dba47b1cfc8a02e16b780c42a",
    "to": "+19052087755",
    "from": "+16182128108",
    "messaging_service_sid": null,
    "body": "Sent from your Twilio trial account - hello from twilio",
    "status": "queued",
    "num_segments": "1",
    "num_media": "0",
    "direction": "outbound-api",
    "api_version": "2010-04-01",
    "price": null,
    "price_unit": "USD",
    "error_code": null,
    "error_message": null,
    "uri": "/2010-04-01/Accounts/ACe164076dba47b1cfc8a02e16b780c42a/Messages/SMc070607afd0d4f668b0e36ede9acebab.json",
    "subresource_uris": {
        "media": "/2010-04-01/Accounts/ACe164076dba47b1cfc8a02e16b780c42a/Messages/SMc070607afd0d4f668b0e36ede9acebab/Media.json"
    }
}
'''