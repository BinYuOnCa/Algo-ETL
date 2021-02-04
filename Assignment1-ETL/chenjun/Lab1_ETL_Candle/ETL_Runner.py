import pandas as pd
import finnhub
import datetime as dt
from execute.copyToDatabase import copy_from_file
from execute.sendSMS import send_sms
from utils.dateTimeHelper import getLastUpdateTS_int, getNextStartingMinute, getNextStartingDay
from utils.dbHelper import get_db_connect, get_next_start_id, connect
from datetime import datetime


def us_equity_candles_ETL_run(interval):
    end_time = int(dt.datetime.now().timestamp())
    start_time = end_time-1
    st = None
    endt = None
    TABLE_NAME = ' '
    STOCK_NAME = 'AAPL'

    try:
        connect()

        # Setup client

        finch_client = finnhub.Client(api_key=API_KEI)

        # Stock candles data from Internet
        if interval == '1m':
            INVT = "1"
            TABLE_NAME = "us_equity_1m_finn"
            start_time = getNextStartingMinute(TABLE_NAME, STOCK_NAME)

        elif interval == 'daily':
            INVT = "D"
            TABLE_NAME = "us_equity_daily_finn"
            start_time = getNextStartingDay(TABLE_NAME, STOCK_NAME)
        else:
            print("bad parameters")
            return 1

        print("interval=" + INVT + ' tableName is ' + TABLE_NAME+ "  .......running now....")

        st = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
        endt = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
        print("running from " + f"{st}" + " to " + f"{endt}")

        res = finch_client.stock_candles(STOCK_NAME, INVT, start_time, end_time)

        # Convert to Pandas Dataframe


        df = pd.DataFrame(res)

        df["symbol"] = 'AAPL'
        df['timestamp'] = [dt.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
                           for x in df['t']]
        df = df.rename({'o': 'open_price', 'c': 'close_price', 'h': 'high_price', 'l': 'low_price', 's': 'status',
                        't': 'date_int_key', 'v': 'volume'}, axis='columns')

        last_max_id = get_next_start_id(get_db_connect(), TABLE_NAME)

        df.index = df.index + last_max_id + 1

        print(df.size)

        copy_from_file(get_db_connect(), df, TABLE_NAME)

        send_sms("Job successfully finished! "
                 + f"{df.size}" + " records fetched for -" + interval +"- " + STOCK_NAME +   " from "+st+ " to " + endt)
        return 0
    except Exception as e:
        print (e)
        print(" ------ no data fetched -----------")
        send_sms("No data fetched for " + interval +" " + STOCK_NAME +   " from "+st+ " to " + endt)
        return 0