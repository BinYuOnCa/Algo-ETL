"""
This Script is designed to extract data from Finnhub.io
"""
import time
import pandas as pd
import requests
import finnhub
from functools import wraps
from datetime import datetime, timezone
from etl_utils.etl_config import FINNHUB_CONFIG
from stack_info import total_lock


def limit_usage(func):
    """
    Use decorator to limit the Finnhub API usage.

    :param func: Extracting Function.
    :return: None
    """
    last_use_time = 0

    @wraps(func)
    def wrapper(*args, **kwargs):
        total_lock.acquire()
        nonlocal last_use_time
        t = time.time() - last_use_time
        if t >= FINNHUB_CONFIG["API_LIMIT"]:
            last_use_time = time.time()
        else:
            time.sleep(FINNHUB_CONFIG["API_LIMIT"] - t + 0.1)
            last_use_time = time.time()
        total_lock.release()
        return func(*args, **kwargs)

    return wrapper


def convert_datetime(date_time):
    """
    Convert the datetime from local timezone to UTC timestamp.

    :param date_time: (datetime/str) 'Y-m-d H:M:S'
    :return: (timestamp)
    """

    if isinstance(date_time, datetime):  # Directly convert to timestamp if input datetime
        dt_stamp = int(date_time.astimezone(timezone.utc).timestamp())
    else:
        try:
            date_time = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            dt_stamp = int(date_time.astimezone(timezone.utc).timestamp())
        except Exception:
            raise Exception(
                "Please keep the input string in proper form as 'Y-m-d H:M:S'.")
    return dt_stamp


@limit_usage
def extract_candles(symbol, dt_start, dt_end, resolution='D'):
    """
    Extract stack candles data through Finnhub API

    :param symbol: (str) Stack abbreviation
    :param dt_start: (datetime)
    :param dt_end: (datetime)
    :param resolution: Supported resolution includes 1, 5, 15, 30, 60, D, W, M.
    Some time frames might not be available depending on the exchange.
    :return: (DataFrame) empty dataframe if false to download
    """
    finnhub_client = finnhub.Client(api_key=FINNHUB_CONFIG["API_KEY"])
    # Download the historical daily data from Finnhub:
    try:
        res = finnhub_client.stock_candles(symbol, resolution,
                                           convert_datetime(dt_start),
                                           convert_datetime(dt_end))
        # print('api called time{}'.format(datetime.today().strftime("%H:%M:%S")))
    except Exception as e:
        print('Sorry, when extract {0} candles, because of {1}, '
              'your request cannot be finished.'.format(symbol, e.__class__))
        return pd.DataFrame()
    else:
        if res['s'] == 'no_data':
            if resolution == '1':
                return pd.DataFrame()
            else:
                print('{0} has no data returned from {1} to {2}.'.format(symbol, dt_start, dt_end))
                return pd.DataFrame()
        else:
            finnhub_data = pd.DataFrame(res)
            finnhub_data["symbol"] = symbol
            # Convert the timestamp column to readable form:
            finnhub_data["t"] = finnhub_data["t"].apply(lambda x:
                                                        datetime.fromtimestamp(x).astimezone(timezone.utc))
            # Make sure the volume column are all integers:
            finnhub_data["v"] = finnhub_data["v"].apply(lambda x: int(x))
            # Rename the columns:
            finnhub_data = finnhub_data.rename({'c': 'close_price',
                                                'h': 'high_price',
                                                'l': 'low_price',
                                                'o': 'open_price',
                                                's': 'status',
                                                't': 'timestamp',
                                                'v': 'volume'}, axis=1)
            return finnhub_data


@limit_usage
def extract_splits(symbol, dt_start, dt_end):
    """
    Extract splits data through Finnhub API.

    :param symbol: (str) Stack abbreviation
    :param dt_start: (datetime)
    :param dt_end: (datetime)
    :return: (DataFrame) empty dataframe if false to download
    """
    # Convert the date inputs to right form:
    if isinstance(dt_start, datetime):
        dt_start = dt_start.astimezone(timezone.utc)
        dt_start = dt_start.strftime('%Y-%m-%d')
    if isinstance(dt_end, datetime):
        dt_end = dt_end.astimezone(timezone.utc)
        dt_end = dt_end.strftime('%Y-%m-%d')
    # Read finnhub keys
    api_token = FINNHUB_CONFIG["API_KEY"]
    api_url = "https://finnhub.io/api/v1/stock/split?symbol={}&from={}&to={}".format(symbol, dt_start, dt_end)
    api_head = {'X-Finnhub-Token': api_token}
    # Download the historical daily data from Finnhub:
    try:
        res = requests.get(api_url, headers=api_head)
        df = pd.DataFrame(res.json())
        if not df.empty:
            df['source'] = 'api'
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        print('Sorry, when extract {0} splits, because of {1}, '
              'your request cannot be finished.'.format(symbol, e.__class__))
        return pd.DataFrame()


def extract_intraday(symbol, dt_start, dt_end, db_table, upload=True):
    """
    Extract intraday candles data from Finnhub. Because the Finnhub API doesn't support to extract intraday data
    has a period longer than 30 days, this function will divide the period into months and combine the result
    together.

    :param symbol: (str) Stack abbreviation
    :param dt_start: (datetime)
    :param dt_end: (datetime)
    :param db_table: (RemoteDatabase)
    :param upload: (boolean) Decide if to upload result directly
    :return: (DataFrame) empty dataframe if false to download
    """
    # Create the time sequence that has a period of 30 days:
    dt_start = dt_start.astimezone(timezone.utc)
    dt_end = dt_end.astimezone(timezone.utc)
    times = pd.date_range(dt_start,
                          dt_end,
                          freq=FINNHUB_CONFIG["INTRADAY_LIMIT"]).strftime("%Y-%m-%d %H:%M:%S").tolist()
    if times[-1] != dt_end:
        times.append(dt_end.strftime("%Y-%m-%d %H:%M:%S"))
    # Extract candles data from Finnhub:
    res_df = pd.DataFrame()
    # Build an interface to Visualize the extracting process
    total_period = len(times) - 1
    for i in range(len(times) - 1):
        print("Creating time series {} of {} for {}".format(i, total_period, symbol))
        # print("extracting {} to {}".format(times[i], times[i + 1]))
        res = extract_candles(symbol, dt_start=times[i], dt_end=times[i + 1], resolution='1')
        if not res.empty:
            res_df = res_df.append(res)
            # Decide if to upload directly
            if upload:
                db_table.update_dataframe(res)

    return res_df
