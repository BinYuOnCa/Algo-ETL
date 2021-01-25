# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 21:31:43 2021
@author: Yingwei Chen
Function: Download stock candle minute data and upload to AWS RDS postgresql database 
"""

import os
import datetime
import finnhub
import time
import smtplib, ssl
from ec2_config import *
from ec2_utility_func import *

def finnhub_client_setup(finnhub_client=None):
    try:
        finnhub_client=finnhub.Client(api_key=finnhub_api_key) if finnhub_client is None else finnhub_client
        return finnhub_client
    except:
        print("Finnhub client is not setup")
        
def finnhub_download_stock_candle(lookup_df, finnhub_data_interval, upto_timestamp):
    stock_candle_list=[]
    for stock_name, start_etl_timestamp in list(zip(lookup_df['symbol'],lookup_df['to_timestamp'])):
        print( stock_name, start_etl_timestamp, upto_timestamp)
        try:
            res_price = finnhub_client.stock_candles(stock_name, finnhub_data_interval, start_etl_timestamp+1, upto_timestamp)
        except:
            print("Extraction Error: Possible Stock is not available for extraction")   
            continue
        try:    
            stock_candle_df=pd.DataFrame(res_price)
            #print(f"{stock_name} records: {len(stock_candle_df.index)} columns: {stock_candle_df.columns}")
            stock_candle_df['symbol']=stock_name
            stock_candle_list.append(stock_candle_df)
            #stock_extract_message+=f"{stock_name}: {len(stock_candle_df.index)} \n"
        except:
            print(f"##### \n !!! Error: There is no stock records extracted!")
        time.sleep(0.1)
    try:
        stock_candle_df_all=pd.concat(stock_candle_list)
        return stock_candle_df_all
    except:
        print(f"###### \n  There are errors when concating or loading finnhub records\n")
        
def finnhub_download_stock_candle_reverse(lookup_df, finnhub_data_interval, from_timestamp):
    stock_candle_list=[]
    for stock_name, start_etl_timestamp in list(zip(lookup_df['symbol'],lookup_df['from_timestamp'])):
        print( stock_name, from_timestamp, start_etl_timestamp)
        try:
            res_price = finnhub_client.stock_candles(stock_name, finnhub_data_interval, from_timestamp, start_etl_timestamp-1)
        except:
            print("Extraction Error: Possible Stock is not available for extraction")   
            continue
        try:    
            stock_candle_df=pd.DataFrame(res_price)
            #print(f"{stock_name} records: {len(stock_candle_df.index)} columns: {stock_candle_df.columns}")
            stock_candle_df['symbol']=stock_name
            stock_candle_list.append(stock_candle_df)
            #stock_extract_message+=f"{stock_name}: {len(stock_candle_df.index)} \n"
        except:
            print(f"##### \n !!! Error: There is no stock records extracted!")
        time.sleep(0.1)
    try:
        stock_candle_df_all=pd.concat(stock_candle_list)
        return stock_candle_df_all
    except:
        print(f"###### \n  There are errors when concating or loading finnhub records\n")    
    
def update_stock_etl_control():
    update_sql_control=''' update finnhub.stock_etl_control as con
                    set from_timestamp=sta.min_time,
                       to_timestamp=sta.max_time
                    from (select symbol,
                                 min(time_finnhub)     as min_time,
                                 max(time_finnhub)     as max_time
                          from finnhub.us_equity_1m_finnhub
                          group by symbol
                        )  as sta
                    where con.symbol=sta.symbol
               '''
    psycopg2_execute_sql(sql_2_exec=update_sql_control)

def transform_finnhub_stock_candle(from_df):
    from_df=from_df[from_df['s']=='ok']
    from_df.drop(columns='s', inplace=True)
    stock_price_rename_list={'c':'close_price','h':'high_price','l':'low_price','o':'open_price','t':'time_finnhub','v':'volume'}
    from_df.rename(columns=stock_price_rename_list, inplace=True)
    from_df['date_int_key']=from_df['time_finnhub'].apply(convert_utc_timestamp_to_yyyymmdd)
    from_df['timestamp']=from_df['time_finnhub'].apply(convert_utc_timestamp_to_daytime)
    return from_df

def upload_copy_from_file(conn, df_upload, db_table, tbl_columns):
    tmp_df="./tmp_dataframe.csv"
    df_upload.to_csv(tmp_df, index=False, header=False)
    f=open(tmp_df, 'r')
    cursor=conn.cursor()
    try:
        cursor.copy_from(f, db_table, sep=',',columns=tbl_columns)
        conn.commit()
    except:
        os.remove(tmp_df)
        print(f"Error with COPY FROM UPLOAD METHOD!")
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
    os.remove(tmp_df)
    
if __name__=='__main__':

    upload_method=2
    
    gmail_message="Subject: Finnhub ETL to AWS Start. \n \n"
    
    twilio_client = Client(twilio_account_sid, twilio_auth_token)
    finnhub_client=finnhub_client_setup()
    sqlalchemy_engine=sqlalchemy_connection(db_connection_url)
    
    stock_minute_columns=['close_price','high_price','low_price', 'open_price', 'time_finnhub','volume' ,'symbol','date_int_key','timestamp']
    lookup_table=sqlalchemy_read_sql(sqlalchemy_engine, "select * from finnhub.stock_etl_control order by to_timestamp")
    print(lookup_table.head())
    # the index of row number can be adjusted to work around the EC2 memory capability. 
    lookup_table_top100=lookup_table.loc[:,:]         
    print(lookup_table_top100)
        
    latest_timestamp=int(datetime.datetime.now().timestamp())
    latest_datetime=convert_utc_timestamp_to_local_datetime(latest_timestamp)
    print(latest_datetime)
    twilio_message(twilio_client, f"The data extraction starts from {latest_datetime} ")
    gmail_message+=f"The data extraction starts from {latest_datetime} \n"
    finnhub_minute_df=finnhub_download_stock_candle(lookup_table_top100, 1, latest_timestamp )
    print(f"There are {len(finnhub_minute_df.index)} extracted with columns: {finnhub_minute_df.columns}")
    finnhub_minute_df=transform_finnhub_stock_candle(finnhub_minute_df)
    #twilio_message(twilio_client, f"There are {len(finnhub_minute_df.index)} extracted.")
    print(f"There are {len(finnhub_minute_df.index)} extracted with columns: {finnhub_minute_df.columns}")

#Slow Method just for demonstration
    if upload_method==1:
        sqlalchemy_to_sql(finnhub_minute_df, sqlalchemy_engine,'finnhub', 'us_equity_1m_finnhub', 'append', 100000)
        twilio_message(twilio_client, f"There are {len(finnhub_minute_df.index)} uploaded.")
        gmail_message+=f"There are {len(finnhub_minute_df.index)} uploaded. \n"
        update_stock_etl_control()
        twilio_message(twilio_client, f"The update to the stock etl control table completed by {convert_utc_timestamp_to_local_datetime(datetime.datetime.now().timestamp())}!")
        gmail_message+=f"The update to the stock etl control table completed by {convert_utc_timestamp_to_local_datetime(datetime.datetime.now().timestamp())}! \n"

# Quick upload Method: adopted here        
    if upload_method==2:
        psycopg2_conn=psycopg2_connection()
        print(finnhub_minute_df.head())
        upload_copy_from_file(psycopg2_conn, finnhub_minute_df, 'finnhub.us_equity_1m_finnhub', stock_minute_columns)
        twilio_message(twilio_client, f"There are {len(finnhub_minute_df.index)} uploaded.")
        gmail_message+=f"There are {len(finnhub_minute_df.index)} uploaded. \n"
        update_stock_etl_control()
        twilio_message(twilio_client, f"The update to the stock etl control table completed by {convert_utc_timestamp_to_local_datetime(datetime.datetime.now().timestamp())}!")
        gmail_message+=f"The update to the stock etl control table completed by {convert_utc_timestamp_to_local_datetime(datetime.datetime.now().timestamp())}! \n"
      
    gmail_algotrading_message(algotrading_email,algotrading_email,465, gmail_password, gmail_message)
   
