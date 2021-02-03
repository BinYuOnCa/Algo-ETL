import os

DB_CONFIG = {
    "DATABASE": os.getenv("DB_NAME"),
    "USER": os.getenv("DB_USERNAME"),
    "PASSWORD": os.getenv("DB_PWD"),
    "HOST": os.getenv("DB_HOST"),
    "PORT": os.getenv("DB_PORT")
}

API_KEY_FINN = {
    "API_KEY_FINN": os.getenv("API_KEY_FINN")
}

TWILIO_CONFIG = {
    "TWILIO_ACCOUNT_SID": os.getenv("TWILIO_ACCOUNT_SID"),
    "TWILIO_AUTH_TOKEN": os.getenv("TWILIO_AUTH_TOKEN"),
    "TWILIO_FROM_PHONE": os.getenv("TWILIO_FROM_PHONE"),
    "TWILIO_TO_PHONE": os.getenv("TWILIO_TO_PHONE")
}

GMAIL_ACCT_CONFIG = {
    "GMAIL_SENDER": os.getenv("GMAIL_SENDER"),
    "GMAIL_SENDER_PWD": os.getenv("GMAIL_SENDER_PWD"),
    "GMAIL_RECEIVER": os.getenv("GMAIL_RECEIVER")
}

# print(DB_CONFIG)
# print(API_KEY_FINN)
