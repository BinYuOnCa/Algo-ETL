# File name: ec2_utility_func.py
# Author   : Yingwei Chen
# Created  : 2021-1-17


import os
import psycopg2
import pandas as pd
import datetime
import smtplib, ssl
from   twilio.rest import Client
from   sqlalchemy import create_engine

#exec(open('/home/ubuntu/algotrading/finnhub_etl/ec2_config.py').read())
from ec2_config import *

#twilio SMS message API
def twilio_message(client=None, messge_content="Test Message"):
    client=Client(twilio_account_sid, twilio_auth_token) #if client is None else client
    message = client.messages.create(
                     body=messge_content,
                     from_=twilio_from,
                     to=twilio_to)
                     
# Gmail Messaging
def gmail_algotrading_message(sender_email, receiver_email, port, password, message):
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email,receiver_email,message)

#psycopg2 sql execution

def psycopg2_connection(psycopg2_conn=None):
    try:
        psycopg2_conn=psycopg2.connect(database=dbname, user=username, password=passwd, host=hostname, port=portname) if psycopg2_conn is None else psycopg2_conn
        return psycopg2_conn
    except (Exception, psycopg2.DatabaseError)  as error:
        print("Error: %s" % error)
        return 1


def psycopg2_execute_sql(psycopg2_conn=None, sql_2_exec=''):
    try:
        psycopg2_conn=psycopg2.connect(database=dbname, user=username, password=passwd, host=hostname, port=portname) if psycopg2_conn is None else psycopg2_conn
    except (Exception, psycopg2.DatabaseError)  as error:
        print("Error: %s" % error)
        return 1
        
    try:
        cursor=psycopg2_conn.cursor()
        cursor.execute(sql_2_exec)
        psycopg2_conn.commit()
    except (Exception, psycopg2.DatabaseError)  as error:
        print("Error: %s" % error)
        #psycopg2_conn.rollback()
        cursor.close()
        return 1
    cursor.close()
    
# sqlalchemy read data from postgresql
    
def sqlalchemy_connection(db_connection_url):
    print(db_connection_url)
    try:
        sqlalchemy_engine=create_engine(db_connection_url)
        return sqlalchemy_engine
    except:
        print("Error: the connection to sqlalchemy not established.")

def sqlalchemy_read_sql(sqlalchemy_engine, sql_2_read):
    try:
        return pd.read_sql(sql_2_read, con=sqlalchemy_engine)
    except:
        print("Read SQL did not return dataframe")
        
def sqlalchemy_to_sql(df, engine, schema_name, table, action, chunk_size):
    try:
        df.to_sql(table, schema=schema_name, con=engine, if_exists=action, index=False, chunksize=chunk_size)
    except:
        print("Write to SQL failed.")
        
def convert_utc_timestamp_to_local_datetime(timestamp, gmt=-5):
    eastern_datetime=datetime.datetime.fromtimestamp(timestamp) + datetime.timedelta(seconds=gmt*3600)
    return eastern_datetime

def convert_utc_timestamp_to_yyyymmdd(timestamp, gmt=-5):
    eastern_datetime=datetime.datetime.fromtimestamp(timestamp) + datetime.timedelta(seconds=gmt*3600)
    yyyymmdd=eastern_datetime.date().year*10000+eastern_datetime.date().month*100+eastern_datetime.date().day
    return yyyymmdd

def convert_utc_timestamp_to_daytime(timestamp, gmt=-5):
    eastern_datetime=datetime.datetime.fromtimestamp(timestamp) + datetime.timedelta(seconds=gmt*3600)
    return eastern_datetime.strftime('%H:%M:%S.%f')

    
if __name__ == '__main__':

    twilio_client = Client(twilio_account_sid, twilio_auth_token)
    twilio_message(twilio_client, 'Unit Test on Twilio Message')

    sql_create_schema='''create schema if not exists TEST_PSYCOPG3'''
    psycopg2_execute_sql(sql_2_exec=sql_create_schema)
     
    pull_control_sql="select * from assignment1.stock_etl_control where status=True"
    sqlalchemy_engine=sqlalchemy_connection(db_connection_url)
    stock_control_etl=sqlalchemy_read_sql(sqlalchemy_engine, pull_control_sql)
    print(stock_control_etl.columns)
    
    latest_timestamp=int(datetime.datetime.now().timestamp())
    latest_datetime=convert_utc_timestamp_to_local_datetime(latest_timestamp)
    print(latest_datetime)

    latest_yyyymmdd=convert_utc_timestamp_to_yyyymmdd(latest_timestamp)
    print(latest_yyyymmdd)
    
    latest_daytime=convert_utc_timestamp_to_daytime(latest_timestamp)
    print(latest_daytime)