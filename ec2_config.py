# File name: ec2_config.py
# Author   : Yingwei Chen
# Created  : 2021-1-17


# AWS RDS connection parameters
dbname='postgres'
username='postgres'
passwd='xing9139'
hostname='awspostgresql.cf4yfwopyfap.us-east-2.rds.amazonaws.com'
portname='5432'
param_dic={"host":hostname, "database": dbname, "user": username, "password":passwd }

# Finnhub API Key
finnhub_api_key="bv8dqn748v6rnm7avf60"
finnhub_sandbox_key="sandbox_bv8dqn748v6rnm7avf6g"

finnhub_stock_candle_minute=1
finnhub_stock_candle_daily='D'

# Twilio credential
twilio_account_sid = 'AC489e96f24732d0a92f43655b1f31fc3a'
twilio_auth_token =  'fb369213c30ae782dc878cf5a8a45d78'
twilio_from='+12512202832',
twilio_to='+16479783581'

# used by sqlalchemy
db_connection_url=f"postgresql://{username}:{passwd}@{hostname}:{portname}/{dbname}"

# for messaging
gmail_password='Xing9139'
algotrading_email="algotradingyingweichen@gmail.com"