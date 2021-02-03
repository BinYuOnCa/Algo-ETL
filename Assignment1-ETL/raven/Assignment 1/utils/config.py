import os
from os.path import join, dirname
from dotenv import load_dotenv
import datetime as dt
import timedelta

# Create .env file path
dotenv_path = join(dirname(__file__), '.env')

# Load file from the path
load_dotenv(dotenv_path)

# Database
DB_CONFIG = {
    "DATABASE": os.getenv('DB_NAME'),
    "USER": os.getenv('DB_USERNAME'),
    "PASSWORD": os.getenv('DB_PWD'),
    "HOST": os.getenv('DB_HOST')
}

# Finnhub
FINNHUB_CONFIG = {
    "FINNHUB_API": os.getenv('API_KEY')
}

# Twilio
TWILIO_CONFIG = {
    "ACCOUNT_SID": os.getenv('TWILIO_ACCOUNT'),
    "AUTH_TOKEN": os.getenv('TWILIO_TOKEN')
}

def end_time(time=None):
    if time is None:
        print(int(dt.datetime.now().timestamp()))
        return (int(dt.datetime.now().timestamp()))
    else:
        print(int(time))
        return int(time)
# End_time = int(dt.datetime.now().timestamp())

def start_time(period,end_time):
    if period == '1m':
        print(int(int(end_time) - int(86400 * 1.5)))
        return (int(int(end_time) - int(86400 * 1.5)))
    elif period == 'daily':
        print(int(int(end_time) - int(31536000 * 1.5)))
        return (int(int(end_time) - int(31536000 * 1.5)))
# Start_time =  int(int(End_time) - int(86400 * 1.5))

# end_time(1600000000)
# start_time('daily')