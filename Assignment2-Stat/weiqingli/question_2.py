import pandas as pd
import configparser
import lib.Tools as tools
from os import path


def get_Question2():

    config = configparser.ConfigParser()
    config.read('./lib/application.config')

    key = config['finnhub']['key']
    resolution = config['question2']['resolution']
    symbol = config['question2']['symbol']
    start_time = config['question2']['start_time']
    end_time = config['question2']['end_time']



    start = tools.date_to_epoch(start_time)
    end = tools.date_to_epoch(end_time)

    if path.exists('./lib/AAPL_Daily_Candle.csv'):
        df = pd.read_csv('./lib/AAPL_Daily_Candle.csv')
    else:
        res = tools.getStockCandles(key, symbol, resolution, start, end)
        res.pop('s', None)
        df = pd.DataFrame(res)
        df.to_csv('./lib/AAPL_Daily_Candle.csv')

    # Insert Date column in df
    df.insert(0, "Date", "")
    # convert epotch time to datetime stamp in Date column
    df['Date'] = pd.to_datetime(df['t'], unit='s')

    # Insert EMA30 column in df
    period = 30
    df.insert(7, "EMA30", float, allow_duplicates=True)
    column_name='EMA30'
    df1 = tools.cal_EMA(period, df, column_name)

    # Insert EMA100 column in df
    period=100
    df1.insert(8, "EMA100", float, allow_duplicates=True)
    column_name='EMA100'
    df2=tools.cal_EMA(period, df1, column_name)
    #print(df)

    return df2

#et_Question2()







