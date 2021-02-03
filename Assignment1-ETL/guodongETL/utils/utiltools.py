import logging
import sys
import time

import pytz
import requests

import finnhub

from datetime import datetime

from utils.settings import API_KEY_FINNHUB


def get_unix_time_from_datetime(d, t=None):
    return int(time.mktime(datetime.strptime((str(d) + t), '%Y%m%d%H:%M:%S').timetuple()))


def get_time_from_unix_timestamp(timestamp):
    return (datetime.fromtimestamp(timestamp)).strftime('%H:%M:%S')


def get_date_from_unix_timestamp(timestamp):
    return (datetime.fromtimestamp(timestamp)).strftime('%Y%m%d')


def get_data_via_finhub_lib(symbol, resolution, start_unix_time, end_unix_time, finnhub_client=None):
    finnhub_client = finnhub.Client(api_key=API_KEY_FINNHUB) if finnhub_client is None else finnhub_client
    try:
        res = finnhub_client.stock_candles(symbol, resolution, start_unix_time, end_unix_time)
        return res
    except Exception as e:
        logging.error(e)
        return None


def get_splits_status_via_finhub_request(symbol, start_date):
    est = pytz.timezone('US/Eastern')
    now = datetime.now().astimezone(est).replace(tzinfo=None)
    current_date = now.strftime('%Y-%m-%d')
    try:
        r = requests.get(
            f'https://finnhub.io/api/v1/stock/split?symbol={symbol}&from={start_date}&to={current_date}'
            f'&token={API_KEY_FINNHUB}')
        return len(r.json()) != 0
    except Exception as e:
        logging.error(e)
        return False


def get_data_via_finhub_request(symbol, resolution, start_unix_time, end_unix_time):
    r = requests.get(
        f'https://finnhub.io/api/v1/stock/candle?symbol={symbol}&resolution={resolution}'
        f'&from={start_unix_time}&to={end_unix_time}&token={API_KEY_FINNHUB}')
    return r.json()


def get_logger():
    date = datetime.now().astimezone(pytz.timezone('US/Eastern')).replace(tzinfo=None).strftime('%Y%m%d')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(f"logs-{date}.log", mode='a')
    handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addHandler(file_handler)
    return logger
