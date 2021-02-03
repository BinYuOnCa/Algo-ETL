# -*- coding: utf-8 -*-
"""
Created on Thu Jan  7 18:46:37 2021

@author: Yingwei Chen
"""
# Create A Schemas Assignment1 and Create tables for ETL Finnhub Stock Candle
# Table assignment1.stock_price_minute: store the Stock Candle data at minute interval; 
# Table assignment1.stock_etl_symbol: control the stocks to ETL from Finnhub to AWS RDS. Four attributes (symbol, start_stamp, end_stamp, stop) 
#       are used to decide what stock to ETL and when to start and stop  
    

# Step 1: Create the Schema and Tables in RDS 

import os
import datetime
from ec2_config import *
from ec2_utility_func import *
#exec(open('/home/ubuntu/algotrading/finnhub_etl/ec2_config.py').read())
#exec(open('/home/ubuntu/algotrading/finnhub_etl/ec2_utility_func.py').read())

sql_create_schema_finnhub='''create schema if not exists finnhub''';

sql_create_tb_price_1m='''create table if not exists finnhub.us_equity_1m_finnhub(
    symbol         varchar(10) not null,
    date_int_key   int,
    timestamp      time,  
    close_price    float,
    high_price     float,
    low_price      float,
    open_price     float,
    time_finnhub   bigint not null,
    volume         bigint
)''';

sql_create_tb_stock_etl_control='''create table if not exists finnhub.stock_etl_control(
    symbol          varchar(10) not null,
    from_timestamp  bigint not null,
    to_timestamp    bigint not null,
    status          boolean not null
)''';

psycopg2_execute_sql(sql_2_exec=sql_create_schema_finnhub)
psycopg2_execute_sql(sql_2_exec=sql_create_tb_price_1m)
psycopg2_execute_sql(sql_2_exec=sql_create_tb_stock_etl_control)

# Step 2.  Select Harry's 1000 stock list to initilize the control table

stock_to_etl=pd.read_csv(r'/home/ubuntu/algotrading/finnhub_etl/sec_list_1000.csv').reset_index()
stock_to_etl.drop(columns='index', inplace=True)
stock_to_etl['from_timestamp']=int(datetime.datetime(2000,1,1,0,0,1).timestamp())
stock_to_etl['to_timestamp']=int(datetime.datetime(2000,1,1,0,0,2).timestamp())
stock_to_etl['status']=True
print(stock_to_etl.head())
print(len(stock_to_etl.index))

sqlalchemy_engine=sqlalchemy_connection(db_connection_url)
sqlalchemy_to_sql(stock_to_etl, sqlalchemy_engine, 'finnhub', 'stock_etl_control', action='replace')

lookup_table=sqlalchemy_read_sql(sqlalchemy_engine, "select * from finnhub.stock_etl_control")
print(lookup_table.head())




