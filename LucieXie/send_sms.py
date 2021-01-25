import os
from twilio.rest import Client
from datetime import datetime
import pytz



account_sid = os.environ['TWILIO_ACCOUNT_SID']
auth_token = os.environ['TWILIO_AUTH_TOKEN']
client = Client(account_sid, auth_token)

def send_msg(msg_type):
    # Setup current time displayed in message @ EST
    utc_datetime = datetime.now()
    local_timezone = pytz.timezone("US/Eastern")
    local_datetime = utc_datetime.replace(tzinfo=pytz.utc)
    local_datetime = local_datetime.astimezone(local_timezone)
    date_time = local_datetime.strftime("%Y-%m-%d %H:%M:%S %Z%z")

    if msg_type == 'ALERT':
        message = client.messages.create(
                                body='ETL ALERT at ' + date_time,
                                from_='+18125779592',
                                to='+16476761015'
                            )
    elif msg_type == 'COMPLETED':
        message = client.messages.create(
                                body='ETL completed at ' + date_time,
                                from_='+18125779592',
                                to='+16476761015'
                            )
    return
    

