# File name: ec2_config.py
# Author   : Yingwei Chen
# Created  : 2021-1-17

# AWS RDS connection parameters
dbname='postgres'
username='postgres'
passwd='*******'
hostname='awspostgresql.cf4yfwopyfap.us-east-2.rds.amazonaws.com'
portname='5432'
param_dic={"host":hostname, "database": dbname, "user": username, "password":passwd }

# Finnhub API Key
finnhub_api_key="**************"
finnhub_sandbox_key="sandbox_*****************"

finnhub_stock_candle_minute=1
finnhub_stock_candle_daily='D'

# Twilio credential
twilio_account_sid = '***************'
twilio_auth_token =  '*******************'
twilio_from='+1222222222',
twilio_to='+16666666666'

# used by sqlalchemy
db_connection_url=f"postgresql://{username}:{passwd}@{hostname}:{portname}/{dbname}"

# for messaging
gmail_password='*******'
algotrading_email="algotradingyingweichen@gmail.com"
