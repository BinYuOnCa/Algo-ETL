import pandas as pd
import numpy as np
from pandas import DataFrame
from sqlalchemy import create_engine
import datetime
import sys
from execution.params import *


def get_latest_datadate(engine):
    """
    get last trading day from database table:daily_prices_new
    :param: engine (db engine object)
    :return (int , YYYYMMDD)
    """
    today = datetime.datetime.today()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")
    input_date = int(str(year+month+day))
    query = f"select max(utc_readable_time) from daily_prices_new where utc_readable_time < {input_date}"
    try:
        result = engine.execute(query)
        r = result.fetchone()[0]
        print(r)
        return r
    except Exception as e:
        # output_log(e, LOG_LEVEL_ERROR)
        return None


def get_engine():
    """
    create database engine for postgres
    :param:
    :return engine: (db engine object)
    """
    engine = create_engine(f'postgresql://postgres:'
                           f'{DB_PASSWORD}@'
                           f'{DB_HOST}/'
                           f'{DB_NAME}')
    return engine


def get_sp500_stock_synbols_list():
    """
    get the S&P 500 stock symbols from wiki
    :param:
    :return (list)
    """
    # get stock symbol
    table = pd.read_html(SP500_SYMBOL_URL)
    df = table[0]
    df = df.sort_values(by='Symbol', ascending=True)
    securities = df['Symbol'].values.tolist()
    return securities


def get_valid_dates(engine, sec, end_date_key, slice_size):
    """
    get valid date list from database table:daily_prices_new
    :param: engine (db engine object)
    :param: sec: (str)
    :param: end_date_key: (int , YYYYMMDD)
    :param: slice_size: (int)
    :return YYYYMMDD: (list)
    """
    print(f"get_valid_dates start ---")
    query = f"select utc_readable_time from daily_prices_new where symbol = '{sec}' " \
            f"and utc_readable_time <= {end_date_key} order by utc_readable_time desc limit {slice_size}"
    try:
        result = engine.execute(query)
        df = DataFrame(result.fetchall(),
                       columns=['utc_readable_time'])
        df = df.sort_values(by='utc_readable_time', ascending=True)
        # print(df)
        return df['utc_readable_time'].values.tolist()
    except Exception as e:
        print(e)
        # output_log(e, LOG_LEVEL_ERROR)
        return None


def download_data_daily(engine,
                        sec,
                        start_date,
                        end_date,
                        interval="daily"):
    """
    Download daily bar data from database table:daily_prices_new
    :param: engine (db engine object)
    :param: sec: (str)
    :param: start_date: (int , YYYYMMDD)
    :param: end_date: (int, YYYYMMDD)
    :param: interval: (str,  "daily")
    :return df: (dataframe)
    """
    print(f"download_data_daily start---")
    if interval == "daily":
        query = f"select symbol, close, high, low, open, utc_readable_time,volume " \
                f"from daily_prices_new where symbol = '{sec}' and " \
                f"utc_readable_time between {start_date} and {end_date}"
    try:
        result = engine.execute(query)
        df = DataFrame(result.fetchall(),
                       columns=['symbol', 'close', 'high', 'low', 'open', 'utc_readable_time', 'volume'])
        df = df.sort_values(by='utc_readable_time', ascending=True)
        df.set_index('utc_readable_time')
        # print(df)
        return df
    except Exception as e:
        print(e)
        # output_log(e, LOG_LEVEL_ERROR)
        return None


def download_data_minutes(engine,
                        sec,
                        start_date,
                        end_date,
                        interval="1min"):
    """
    Download minutes bar data from database table:min_prices
    :param: engine (db engine object)
    :param: sec: (str)
    :param: start_date: (int , YYYYMMDD)
    :param: end_date: (int, YYYYMMDD)
    :param: interval: (str,  "1min")
    :return df: (dataframe)
    """
    print(f"download_data_minutes start---")
    if interval == "1min":
        query = f"select symbol, close, high, low, open, utc_unix_time,local_time, volume " \
                f"from min_prices where symbol = '{sec}' and " \
                f"utc_unix_time between {start_date} and {end_date}"
    try:
        result = engine.execute(query)
        df = DataFrame(result.fetchall(),
                       columns=['symbol', 'close', 'high', 'low', 'open', 'utc_unix_time', 'local_time', 'volume'])
        df = df.sort_values(by='local_time', ascending=True)
        df.set_index('local_time')
        # print(df)
        return df
    except Exception as e:
        print(e)
        # output_log(e, LOG_LEVEL_ERROR)
        return None


def three_plus_one_bin_slice(engine, sec: str, date_key_int: int, valid_dates: list, slice_size: int):
    """
    Download minutes bar data from database table:min_prices
    :param: engine (db engine object)
    :param: sec: (str)
    :param: start_date: (int , YYYYMMDD)
    :param: end_date: (int, YYYYMMDD)
    :param: interval: (str,  "1min")
    :return list: (sec : str, log_return : float32)
    """
    print(f"three_plus_one_bin_slice start---")
    try:
        idx_ori = valid_dates.index(date_key_int)
        # print(f"sec = {sec} , date_key_int= {date_key_int} , idx_ori = {idx_ori}")
    except Exception as e:
        print(f"date {date_key_int} does not exist in valid dates")
        return False, None
    idx_d1 = idx_ori - 3 * slice_size + 1
    # print(f"sec = {sec} , start_date= {valid_dates[idx_d1]} , idx_d1 = {idx_d1}")
    daily_data = download_data_daily(engine,
                                     sec,
                                     start_date=valid_dates[idx_d1],
                                     end_date=date_key_int,
                                     interval="daily")
    # print(daily_data)
    daily_data['close_yesterday'] = daily_data['close'].shift()
    daily_data['log_return'] = np.log(daily_data['close'] / daily_data['close_yesterday'])
    d1_high = daily_data['high'].iloc[0:slice_size-1].max()
    d2_high = daily_data['high'].iloc[slice_size:2 * slice_size-1].max()
    d3_high = daily_data['high'].iloc[2 * slice_size:3 * slice_size-1].max()

    d1_low = daily_data['low'].iloc[0:slice_size-1].min()
    d2_low = daily_data['low'].iloc[slice_size:2 * slice_size-1].min()
    d3_low = daily_data['low'].iloc[2 * slice_size:3 * slice_size-1].min()
    #     print(d1_high, d2_high, d3_high, d1_low, d2_low, d3_low)
    result = d1_high < d2_high < d3_high and d1_low < d2_low < d3_low
    last_date_turnover = round(daily_data['close'].iloc[3 * slice_size-1]
                               * daily_data['volume'].iloc[3 * slice_size-1], 2)
    total_log_return = round(np.cumsum(daily_data['log_return']).iloc[-1], 4)
    if result:
        print(f"d1_high={d1_high} , "
              f"d2_high={d2_high} , "
              f"d3_high={d3_high} and "
              f"d1_low={d1_low} , "
              f"d2_low={d2_low}, "
              f"d3_low={d3_low}")
        print(f"sec={sec} , log_return= {total_log_return}")
    return result, last_date_turnover, total_log_return


def get_sec_list_fit_three_plus_one_model(engine, secs, end_date_key, slice_size):
    """
    Download minutes bar data from database table:min_prices
    :param: engine (db engine object)
    :param: sec: (str)
    :param: end_date: (int, YYYYMMDD)
    :param: slice_size: (int)
    :return list: (security_name : str, turnover : float32 , log_return : float32)
    """
    print(f"get_sec_list_fit_three_plus_one_model start ---")
    secs_fit_three_plus_one = []
    for sec in secs:
        valid_dates = get_valid_dates(engine, sec, end_date_key, slice_size*3)
        # print(f"sec= {sec} ,valid_dates = {valid_dates} ")
        if valid_dates and len(valid_dates) == slice_size * 3:
            status, turnover, log_return = three_plus_one_bin_slice(engine, sec, end_date_key, valid_dates, slice_size)
            if status == True:
                secs_fit_three_plus_one.append({'security_name': sec, 'turnover': turnover , 'log_return': log_return})
    secs_fit_three_plus_one = sorted(secs_fit_three_plus_one, key=lambda k: k['log_return'], reverse=True)
    df = pd.DataFrame(secs_fit_three_plus_one, columns=['security_name', 'turnover', 'log_return'])
    df.to_csv('secs_list.csv', index=False)
    print(f"final secs_fit_three_plus_one:{secs_fit_three_plus_one}")
    return secs_fit_three_plus_one


def calculate_normalized_ATR(engine, sec, date_key_int, valid_dates, period):
    """
    Calculate the normalized ATR
    :param: engine: (db engine object)
    :param: sec: (str)
    :param: date_key_int: (int, YYYYMMDD)
    :param: valid_dates: (list, element in list: YYYYMMDD)
    :param: period: (int)
    :return status:(boolean), normalized_atr: (float32)
    """
    print(f"calculate_Normalized_ATR start---")
    try:
        idx_ori = valid_dates.index(date_key_int)
        # print(f"sec = {sec} , date_key_int= {date_key_int} , idx_ori = {idx_ori}")
    except Exception as e:
        print(f"date {date_key_int} does not exist in valid dates")
        return False, None
    idx_d1 = idx_ori - period + 1
    # print(f"sec = {sec} , start_date= {valid_dates[idx_d1]} , idx_d1 = {idx_d1}")
    df = download_data_daily(engine,
                             sec,
                             start_date=valid_dates[idx_d1],
                             end_date=date_key_int,
                             interval="daily")
    # print(df)
    df['H-L'] = abs(df['high']-df['low'])
    df['H-PC'] = abs(df['high']-df['close'].shift(1))
    df['L-PC'] = abs(df['low']-df['close'].shift(1))
    df['TR'] = df[['H-L','H-PC','L-PC']].max(axis=1)
    df['ATR'] = np.nan
    df.loc[period-1,'ATR'] = df['TR'][:period-1].mean()
    for i in range(period,len(df)):
        df['ATR'][i] = (df['ATR'][i-1]*(period-1)+ df['TR'][i])/period
    normalized_atr = round(df['ATR'].iloc[-1] / df['close'].iloc[-1], 4)
    return True, normalized_atr


def calculate_model1_buy_thd(engine, secs, end_date_key, interval=14):
    """
    Calculate model1 buy threshold
    :param: engine: (db engine object)
    :param: secs: (list of str)
    :param: end_date_key: (int, YYYYMMDD)
    :param: interval: (int)
    :return status:(boolean), normalized_atr: (float32), buy_thd: (float32)
    """
    print(f"calculate_model1_buy_thd start ---")
    secs_buy_thd = []
    for sec in secs:
        valid_dates = get_valid_dates(engine, sec, end_date_key, 14)
        # print(f"sec= {sec} ,valid_dates = {valid_dates} ")
        if valid_dates and len(valid_dates) == 14:
            status, ATR = calculate_normalized_ATR(engine, sec, end_date_key, valid_dates,14)
            if status:
                secs_buy_thd.append({'security_name': sec, 'ATR': ATR})
    secs_buy_thd = sorted(secs_buy_thd, key=lambda k: k['ATR'], reverse=True)
    df2 = pd.DataFrame(secs_buy_thd, columns=['security_name', 'ATR'])
    df2['buy_thd'] = round(INVESTMENT_AMT * df2['ATR'].mean() / df2['ATR'] , 2)
    df2.to_csv('secs_buy_thd.csv', index=False)
    print(f"final secs_buy_thd:{secs_buy_thd}")
    return secs_buy_thd
