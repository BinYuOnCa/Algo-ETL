import os
from twilio.rest import Client

# Find these values at https://twilio.com/user/account
# To set up environmental variables, see http://twil.io/secure

def message_me():
    account_sid = 'AC2bc3ccea8d8e94c455db28223835cc71'
    auth_token = '9435f834a22a170aca53d72fa36c382e'

    client = Client(account_sid, auth_token)

    client.api.account.messages.create(
        to="+16473620959",
        from_="2897971628",
        body="Just complete the stock price update!")