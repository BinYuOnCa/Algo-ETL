#!/bin/bash

source /home/ubuntu/.bash_profile

echo $DB_NAME

export API_KEY_FINN=$API_KEY_FINN

export DB_NAME=$DB_NAME
export DB_USERNAME=$DB_USERNAME
export DB_PWD=$DB_PWD
export DB_HOST=$DB_HOST
export DB_PORT=$DB_PORT

export TWILIO_ACCOUNT_SID=$TWILIO_ACCOUNT_SID
export TWILIO_AUTH_TOKEN=$TWILIO_AUTH_TOKEN
export TWILIO_FROM_PHONE=$TWILIO_FROM_PHONE
export TWILIO_TO_PHONE=$TWILIO_TO_PHONE

export GMAIL_RECEIVER=$GMAIL_RECEIVER
export GMAIL_SENDER_PWD=$GMAIL_SENDER_PWD
export GMAIL_SENDER=$GMAIL_SENDER



/home/ubuntu/anaconda3/bin/python /home/ubuntu/DS_project/finnhub_etl.py

