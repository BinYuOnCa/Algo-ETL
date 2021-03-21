import pandas as pd
import configparser
import lib.Tools as tools
from os import path

def get_Question3():
    if path.exists('./lib/AAPL_Daily_Candle.csv'):
        df = pd.read_csv('./lib/AAPL_Daily_Candle.csv')
    else:
        config = configparser.ConfigParser()
        config.read('./lib/application.config')
        key = config['finnhub']['key']
        resolution = config['question2']['resolution']
        symbol = config['question2']['symbol']
        start_time = config['question2']['start_time']
        end_time = config['question2']['end_time']

        start = tools.date_to_epoch(start_time)
        end = tools.date_to_epoch(end_time)
        res = tools.getStockCandles(key, symbol, resolution, start, end)
        res.pop('s', None)
        df = pd.DataFrame(res)
        df.to_csv('./lib/AAPL_Daily_Candle.csv')


    # Insert Date column in df
    df.insert(0, "Date", "")
    # convert epotch time to datetime stamp in Date column
    df['Date'] = pd.to_datetime(df['t'], unit='s')
    df.insert(7, "daily_return", float, allow_duplicates=True)

    df1 = tools.cal_DailyReturn(df)

    return df1




