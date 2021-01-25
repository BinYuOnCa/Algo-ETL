import os
from twilio.rest import Client

def sendMessage(body):
    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    from_number = os.getenv("TWILIO_FROM_NUMBER")
    to_number = os.getenv("TWILIO_TO_NUMBER")

    client = Client(account_sid, auth_token)
    message = client.messages.create(
        body="Daily ETL job done!",
        from_=from_number,
        to=to_number
    )
    return message


