"""
This script is to set user configurations.
"""
import os
from os.path import join, dirname

from dateutil import tz
from dotenv import load_dotenv

# Automatically find .env file and load.
env_path = join(dirname(__file__), 'etl_info.env')
print(env_path)
load_dotenv(env_path)

# Load RDS information:
RDS_CONFIG = {
    "USERNAME": os.getenv("RDS_USER"),
    "PASSWORD": os.getenv("RDS_PASSWORD"),
    "HOST": os.getenv("RDS_HOST"),
    "DATABASE": os.getenv("RDS_DATABASE"),
    "NAME": os.getenv("RDS_NAME"),
    "PORT": os.getenv("RDS_PORT"),
    # ----- CUSTOM PART -----
    "DAILY_TABLE": 'daily_raw',
    "INTRADAY_TABLE": 'intraday_raw',
    "SPLIT_TABLE": 'split_ref',
    "CHUNK_SIZE": 100000
}

# Load Finnhub information:
FINNHUB_CONFIG = {
    "API_KEY": os.getenv("FINNHUB_API_KEY"),
    # ----- CUSTOM PART -----
    "API_LIMIT": 1.25,  # control how long time that the API can be used again.
    "INTRADAY_LIMIT": '30D'  # Finnhub limits the intraday data return period as 30 days.
}

# Load the Twilio information:
ALERT_CONFIG = {
    "ACCOUNT_SID": os.getenv("TWILIO_ACCOUNT_SID"),
    "AUTH_TOKEN": os.getenv("TWILIO_AUTH_TOKEN"),
    "TWILIO_PHONE": os.getenv("TWILIO_PHONE"),
    "USER_PHONE": os.getenv("USER_PHONE"),
    "EMAIL_SENDER_NAME": os.getenv("EMAIL_SENDER_NAME"),
    "EMAIL_SENDER_PWD": os.getenv("EMAIL_SENDER_PWD"),
    "EMAIL_RECEIVER": os.getenv("EMAIL_RECEIVER")
}

# Set user information:
USER_CUSTOM = {
    "TIMEZONE": tz.gettz("Canada/Toronto"),
    "FIRST_RUN": False,  # Run the ETL process at first time or not
    "CHECK_HOUR": 1,  # How many hours later to check the intraday data
    "POSTPONE": 24,  # How many hours delayed to extract data from Finnhub to avoid inconsistent data
    "T_LEVEL": 0.005,  # Tolerance level to the inconsistent data
    "T_NUMBER": 1000,  # The threshold number to judge the inconsistent data
    "ALERT": True,  # Allow the email and message services
    "MULTILINE": True  # Allow the process run parallel

}
