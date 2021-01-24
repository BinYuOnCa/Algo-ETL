import finnhub
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from .config import column_names, intraday_limit, resolution, cred_info
from .time_processor import timeframe_gen, add_days

def download_candle(symbol,
                    interval,
                    end_date,
                    start_date=None,
                    recent_dates=None,
                    api_key=cred_info['api_key']):
    '''
    downloads candles from finnhub api
    :param: symbol: (Str)
    :param: interval: 1m or 1d
    :param: end_date: datetime object
    :param: start_date: datetime object
    :param: recent_dates: (dict Str Int)
    :param: api_key: (Str)
    :return: pandas df
    '''
    # Set client
    finnhub_client = finnhub.Client(api_key=api_key)
    # Set the generator to give timeframe in unix time
    if start_date is None:
        start_date = add_days(recent_dates[symbol][0], 1)
        print(start_date)
    increment = intraday_limit if interval == '1m' else None
    timeframe_generator = timeframe_gen(start=start_date,
                                        end=end_date,
                                        n=increment)
    # Empty df to store candles
    candles_df = pd.DataFrame()
    for time_frame in timeframe_generator:
        # timer to avoid api limit
        timer = time.time()
        # download
        raw_candles = finnhub_client.stock_candles(symbol,
                                                   resolution[interval],
                                                   time_frame['start'],
                                                   time_frame['end'])
        del raw_candles['s']
        # add symbol key if the candles exist
        if len(raw_candles):
            raw_candles['symbol'] = symbol
            candles_df = candles_df.append(pd.DataFrame(raw_candles), ignore_index=True)
        elapse = time.time() - timer
        # Sleep until 1s is passed
        time.sleep(max(1 - elapse, 0))

    return candles_df

