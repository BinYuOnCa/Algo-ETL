import pandas as pd
import configparser
import lib.Tools as tools
from os import path


def get_Question1():
    if path.exists('./lib/AAPL_1Day_Candle.csv'):
        df = pd.read_csv('./lib/AAPL_1Day_Candle.csv')
    else:
        config = configparser.ConfigParser()
        config.read('./lib/application.config')

        key = config['finnhub']['key']
        resolution = config['finnhub']['resolution']
        symbol = config['finnhub']['symbol']
        start_time = config['finnhub']['start_time']
        end_time = config['finnhub']['end_time']

        start = tools.date_to_epoch(start_time)
        end = tools.date_to_epoch(end_time)
        res = tools.getStockCandles(key, symbol, resolution, start, end)
        res.pop('s', None)
        df = pd.DataFrame(res)
        df.to_csv('./lib/AAPL_1Day_Candle.csv')

    # Insert Date column in df
    df.insert(0, "Date", "")
    # convert epotch time to datetime stamp in Date column
    df['Date'] = pd.to_datetime(df['t'], unit='s')

    df.insert(7, "avg_price", float, allow_duplicates=True)
    df["avg_price"]=(df['h']+df['l']+df['c'])/3

    df.insert(8, "cumulative_volume", int, allow_duplicates=True)
    df.insert(9, "avg_price_volume", float, allow_duplicates=True)
    df.insert(10, "cumulative_price_volume", float, allow_duplicates=True)
    df.insert(11, "VWAP", float, allow_duplicates=True)

    for index in df.index:
        df.at[index, 'avg_price_volume'] = df.at[index, 'v'] * df.at[index, 'avg_price']
        if index == 0:
            df.at[index, 'cumulative_volume'] = df.at[index, 'v']
            df.at[index, 'cumulative_price_volume'] = df.at[index, 'avg_price_volume']
        else:
            df.at[index, 'cumulative_volume'] = df.at[index - 1, 'cumulative_volume'] + df.at[index, 'v']
            df.at[index, 'cumulative_price_volume'] = df.at[index - 1, 'cumulative_price_volume'] + df.at[
                index, 'avg_price_volume']
        df.at[index, 'VWAP'] = df.at[index, 'cumulative_price_volume'] / df.at[index, 'cumulative_volume']

    return df



#get_Question1()