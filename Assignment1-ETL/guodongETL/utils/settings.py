import os
from os.path import join, dirname

from dotenv import load_dotenv

env_path = join(dirname(__file__), '.env')

load_dotenv(env_path)

DEV_DB_CONFIG = {
    "HOST": os.getenv("DB_HOST"),
    "PORT": os.getenv("DB_PORT"),
    "DATABASE": os.getenv("DB_NAME"),
    "USER": os.getenv("DB_USER"),
    "PASSWORD": os.getenv("DB_PASSWORD")
}

API_KEY_FINNHUB = os.getenv("API_KEY_FINNHUB")

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_SMS_NUMBER = os.getenv("TWILIO_SMS_NUMBER")
RECEIVER_NUMBER = os.getenv("RECEIVER_NUMBER")

US_EQUITY_DAILY_FINN_TABLE = os.getenv("US_EQUITY_DAILY_FINN_TABLE")
US_EQUITY_1M_FINN_TABLE = os.getenv("US_EQUITY_1M_FINN_TABLE")

EMAIL_ACCOUNT = os.getenv("EMAIL_ACCOUNT")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
