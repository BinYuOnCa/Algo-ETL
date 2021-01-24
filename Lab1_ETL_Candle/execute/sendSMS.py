
import os
from twilio.rest import Client


# /usr/bin/env python
# Download the twilio-python library from twilio.com/docs/libraries/python
import os
from twilio.rest import Client

# Find these values at https://twilio.com/user/account
# To set up environmental variables, see http://twil.io/secure
#_account_sid = os.environ['TWILIO_ACCOUNT_SID']
#_auth_token = os.environ['TWILIO_AUTH_TOKEN']



# send sms
def send_sms(msg):
    account_sid = "AC7857d738ad4ffbdecbc7f774c7218091"
    auth_token = "d8d703f5d7bae461c4a3c1c35920cba4"

    client = Client(account_sid, auth_token)

    message = client.messages.create(
        to="+16474568590",
        from_="+18654090686",
        body=msg)

    print(message)

