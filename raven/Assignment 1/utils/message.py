from twilio.rest import Client
import datetime as dt
try:
    from config import TWILIO_CONFIG
except ModuleNotFoundError:
    from .config import TWILIO_CONFIG

def Send_sms():
    client = Client(TWILIO_CONFIG["ACCOUNT_SID"],TWILIO_CONFIG["AUTH_TOKEN"])
    print("Twilio connected")
    message = client.messages\
        .create(
        body="ETL completed at "+str(dt.datetime.now()),
        from_="+12349013647",to="+16479847888")
    print("Message sent")

def error_sms():
    client = Client(TWILIO_CONFIG["ACCOUNT_SID"],TWILIO_CONFIG["AUTH_TOKEN"])
    message = client.messages \
        .create(
        body="Error happen at " + str(dt.datetime.now()),
        from_="+12349013647", to="+16479847888")
    print("Message sent")
