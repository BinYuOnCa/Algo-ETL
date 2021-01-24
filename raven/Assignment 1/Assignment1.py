import requests
import finnhub
import datetime as dt
import timedelta
import pandas as pd
import psycopg2
from twilio.rest import Client
from utils.message import Send_sms
from utils.config import *
from utils.db_util import *
from utils.finnhub_util import *
import os

# requests
# Quote
# quote = requests.get('https://finnhub.io/api/v1/quote?' +
#                      'symbol=BILI&token=bvssucf48v6rku8bbp4g')
# bili = quote.json()
# print(bili)
# date = dt.datetime.fromtimestamp(bili['t'])
# print (date)
#
# #Candles
# candles = requests.get('https://finnhub.io/api/v1/stock/candle?' +
#                        'symbol=BILI&resolution=1' +
#                        '&from=1609459200&to=1610209066' +
#                        '&token=bvssucf48v6rku8bbp4g')
# bili_candles = pd.DataFrame(candles.json())
# print(bili_candles.head())
# bili_candles['ts'] = [dt.datetime.fromtimestamp(x)
#                       for x in bili_candles['t']]
# print(bili_candles.head())

print('*' * 70)
# finbhub-python
# quote
# connect_to_finnhub()
# print("Finnhub connected")

period = '1m'

conn = connect_to_db()


# print("Database connected")
watch_list = get_stock_list(conn)

# cur1 = conn.cursor()
# cur2 = conn.cursor()
# cur1.execute("SELECT symbol FROM stock_list_test")
# records = cur1.fetchall()
# print(records)

# datetime_end = int(dt.datetime.now().timestamp())
datetime_end = end_time()
datetime_start = start_time(period,datetime_end)
# datetime_start = start_time(end_time())
# datetime_start = int(datetime_end) - int(86400*1.5)
print(datetime_start,datetime_end)

# get_stock_list()

for row in watch_list:

# bili_quote = finnhub_client.quote('BILI')
# print(bili_quote)
#print(dt.datetime.fromtimestamp(bili_quote['t']))
#int((dt.datetime.now()-dt.timedelta(seconds=60)).timestamp())
# datetime_end = int(dt.datetime.now().timestamp())
# datetime_start = int(datetime_end) - int(86400*1.5)
# print(datetime_start,datetime_end)
# candles
#Step 1 - Extract from finnhub

    stock = row[0]
    print(stock)
    try:
        # res = finnhub_client.stock_candles(
        #     stock, '1', datetime_start, datetime_end)
        # stock_candles = pd.DataFrame.from_records(res)
        # print(stock_candles)
        # stock_candles = download_1m_finn(stock, datetime_start, datetime_end)

# bili_candles['dt'] = [dt.datetime.fromtimestamp(x)
#                       for x in bili_candles['t']]
# print(bili_candles)

#Step 2 - Load into database
# conn = psycopg2.connect(
#     database="algotrading", user="postgres", password="1987612ren-",
#     host="algotrading.csfjwwgwtmkm.ca-central-1.rds.amazonaws.com", )
# print("Database connected")
# cur = conn.cursor()

        if period == '1m':
#         stock_candles = pd.DataFrame.from_records(download_1m_finn(stock, datetime_start, datetime_end))
            stock_candles = pd.DataFrame.from_records(download_1m_finn(stock, datetime_start, datetime_end))

            for index, row in stock_candles.iterrows():
                load_into_1m_db(conn, stock, row.o, row.c, row.h, row.l, row.v, dt.datetime.fromtimestamp(row.t))
        elif period == 'daily':
            stock_candles = pd.DataFrame.from_records(download_daily_finn(stock, datetime_start, datetime_end))

            for index, row in stock_candles.iterrows():
                load_into_daily_db(conn, stock, row.o, row.c, row.h, row.l, row.v, dt.datetime.fromtimestamp(row.t))
            # sql_query = "INSERT INTO price_test (stock_symbol, price, volumn, datetime) values('"+str(stock)+"',"+str(row.c)+","+str(row.v)+",'"+str(dt.datetime.fromtimestamp(row.t))+"')"
            # print(sql_query)
            # cur2.execute(sql_query)
            # conn.commit()
            # print("Value inserted")
    except ValueError:
        print('No data found. Please try again')

disconnect_db(conn)
print("Database disconnected")

#Step 3 - Sending message
Send_sms()
print("ETL completed for period from "+datetime_start+" to "+datetime_end)