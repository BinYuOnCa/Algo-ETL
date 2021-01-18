from datetime import datetime
import pandas as pd
from pathlib import Path
import requests

import finnhub

import config.config_parser as conf
import utils.convert_time_timestamp as ctt

api_key = conf.finnhub()["api_key"]
# Setup client
finnhub_client = finnhub.Client(api_key=api_key)


def get_candles_df(ticker, candle_freq="D", start_timestamp=None,
                   stop_timestamp=None, finnhub_client=finnhub_client):
    """
    Return stock candles DataFrame for the selected time range and frequency
    :param ticker: string
    :param candle_freq: D or 1
    :param start_timestamp: int
    :param stop_timestamp: int
    :param finnhub_client: object
    :return: Return None if no data
    """

    try:
        if start_timestamp is None:
            start_timestamp = ctt.convert_datetime_timestamp(
                ctt.get_past_date(_date=datetime.today(), years=25))
        if stop_timestamp is None:
            stop_timestamp = ctt.convert_datetime_timestamp(datetime.today())
        finnhub_candles = finnhub_client.stock_candles(ticker, candle_freq, start_timestamp, stop_timestamp)
        stock_candles = pd.DataFrame(finnhub_candles)
        if stock_candles["s"].iloc[0] == "ok":
            stock_candles["symbol"] = str(ticker)
            stock_candles["date_int_key"] = \
                stock_candles["t"].apply(ctt.convert_timestamp_datetime).apply(datetime.date)
            stock_candles["timestamp"] = \
                stock_candles["t"].apply(ctt.convert_timestamp_datetime).apply(datetime.time)
            # stock_candles["exchange"] = str(exchange)
            stock_candles.drop(columns=['s'], inplace=True)
            stock_candles = stock_candles.reindex(
                columns=['symbol', "date_int_key", "timestamp", "o", "c", "h", "l", "v", "t"])
            stock_candles.rename(
                columns={"h": "high_price", "c": "close_price", "l": "low_price", "o": "open_price",
                         "t": "finn_timestamp", "v": "volume"}, inplace=True)
        else:
            stock_candles = None
        return stock_candles
    except Exception as e:
        with open(Path(__file__).parent / "../logs/finn_log.log", "a") as f:
            f.write('"' + str(datetime.today()) +
                    '", "Something is wrong when executing the get_candles_df", "' + str(e) + '"\n')

def get_split_df(ticker, start_date=None, to_date=None, api_key=api_key):
    """
    Return the DataFrame of split info
    :param ticker: string
    :param start_date: string date "YYYY-MM-DD"
    :param to_date:
    :param api_key:
    :return: split DataFrame
    """
    try:
        if start_date is None:
            start_date = ctt.get_past_date(_date=datetime.today(), years=25)
        if to_date is None:
            to_date = datetime.today()
        r = requests.get(
            f'https://finnhub.io/api/v1/stock/split?symbol={ticker}&from={start_date}&to={to_date}&token={api_key}')
        if len(r.json()) != 0:
            return pd.DataFrame(r.json())# print(r.json())
        else:
            return None
    except Exception as e:
        with open(Path(__file__).parent / "../logs/finn_log.log", "a") as f:
            f.write('"' + str(datetime.today()) +
                    '", "Something is wrong when executing the get_split_df", "' + str(e) + '"\n')
