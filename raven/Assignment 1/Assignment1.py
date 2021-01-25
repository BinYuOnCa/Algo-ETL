import datetime as dt
import pandas as pd
from twilio.rest import Client
from utils.message import Send_sms
from utils.config import *
from utils.db_util import *
from utils.finnhub_util import *

period = '1m'

conn = connect_to_db()
# print("Database connected")

watch_list = get_stock_list(conn)

datetime_end = end_time()
datetime_start = start_time(period,datetime_end)
print(datetime_start,datetime_end)

for row in watch_list:
    stock = row[0]
    print(stock)
    try:
        if period == '1m':
            stock_candles = pd.DataFrame.from_records(download_1m_finn(stock, datetime_start, datetime_end))
            for index, row in stock_candles.iterrows():
                load_into_1m_db(conn, stock, row.o, row.c, row.h, row.l, row.v, dt.datetime.fromtimestamp(row.t))
        elif period == 'daily':
            stock_candles = pd.DataFrame.from_records(download_daily_finn(stock, datetime_start, datetime_end))

            for index, row in stock_candles.iterrows():
                load_into_daily_db(conn, stock, row.o, row.c, row.h, row.l, row.v, dt.datetime.fromtimestamp(row.t))
    except ValueError:
        print('No data found. Please try again')

disconnect_db(conn)
print("Database disconnected")

#Step 3 - Sending message
Send_sms()
print("ETL completed for " + period + " from " + datetime_start + " to " + datetime_end)
