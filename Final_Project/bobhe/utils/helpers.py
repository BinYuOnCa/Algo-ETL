from execution.params import *
import pandas as pd
import numpy as np
import datetime


def market_opened(time_zone_adjust, min_bfr = None):
    """
    check market if opened
    :param: time_zone_adjust (boolean)
    :param: min_bfr (int)
    :return True or False (boolean)
    """
    now = datetime.datetime.now()
    # print(f"now={now}")
    H1 = int(now.strftime("%H"))
    M1 = int(now.strftime("%M"))
    S1 = int(now.strftime("%S"))
    market_open_time = datetime.time(9, 30)
    # print(f"market_open_time={market_open_time}")
    H2 = int(market_open_time.strftime("%H"))
    M2 = int(market_open_time.strftime("%M"))
    S2 = int(market_open_time.strftime("%S"))
    if min_bfr is None and ((H1==H2 and M1==M2 and S1>=S2) or (H1==H2 and M1>M2) or H1>H2):
        return True
    elif (H1==H2 and ( 0 <= M2-M1 <= min_bfr)) or (H1==H2 and M1>=M2) or (H1==H2 and M1==M2 and (S1 >= S2)) or H1>H2:
        return True
    else:
        return False


def read_sec_list(file_name):
    """
    read security list form csv
    :param: file_name (csv)
    :return df (dataframe)
    """
    df = pd.read_csv(file_name)
    d = df.to_dict('records')
    return d


def calculate_atr(df, period=14):
    """
    calculate ATR
    :param: df (dataframe)
    :param: period (int, default 14)
    :return df (dataframe)
    """
    print(f"calculate_intraday_normalized_ATR start---")
    df['H-L'] = abs(df['high'] - df['low'])
    df['H-PC'] = abs(df['high'] - df['close'].shift(1))
    df['L-PC'] = abs(df['low'] - df['close'].shift(1))
    df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
    df['ATR'] = np.nan
    df.loc[df.index[period - 1], 'ATR'] = df['TR'][:period - 1].mean()
    for i in range(period, len(df)):
        # df['ATR'][i] = (df['ATR'][i-1]*(period-1)+ df['TR'][i])/period
        df.loc[df.index[i], 'ATR'] = (df.loc[df.index[i - 1], 'ATR'] * (period - 1) + df.loc[
            df.index[i], 'TR']) / period
    df['ATR_ratio'] = df['ATR'] / df['close']
    # pd.set_option('display.max_columns', None)
    # print(df.tail())
    return df


def calculate_vwap(df):
    """
    calculate vwap
    :param: df (dataframe)
    :return df (dataframe)
    """
    if not DATA_SUBSCRIPTION:
        df['average'] = (df.high + df.low + df.close) / 3
    df['vwap'] = (df.volume * df.average).cumsum() / df.volume.cumsum()
    # pd.set_option('display.max_columns', None)
    # print(df.tail())
    return df


def check_up_trend_patten_higher_high_higher_low(df, slice_size):
    """
    check if stock is in a uptrend patten
    :param: df (dataframe)
    :param: slice_size (int)
    :return True or False (boolean)
    """
    print(f"check_higher_high_higher_low start---")
    if len(df) > 3 * slice_size:
        sec = df['symbol'].iloc[0]
        start_time = df['date'].iloc[0]
        for i in range(len(df) - 3*slice_size):
            # print(f"i = {i}")
            d1_high = df['high'].iloc[i:i+slice_size].max()
            d2_high = df['high'].iloc[i+slice_size:i+2*slice_size].max()
            d3_high = df['high'].iloc[i+2*slice_size:i+3*slice_size].max()
            d1_low = df['low'].iloc[i:i+slice_size].min()
            d2_low = df['low'].iloc[i+slice_size:i+2*slice_size].min()
            d3_low = df['low'].iloc[i+2*slice_size:i+3*slice_size].min()
            vwap = df['vwap'].iloc[i+3*slice_size]
            d3_close = df['close'].iloc[i+3*slice_size]
            result = d1_high < d2_high < d3_high and d1_low < d2_low < d3_low and d1_low < vwap < d3_close
            time = df['date'].iloc[i+3*slice_size]
            if result:
                print(f"sec={sec}, time = {time} , buy opportunity = {result} ~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                return result
    return False


def cal_max_price_after_bought(df, bought_time):
    """
    calcuate the max price after the stock has been brought successfully
    :param: df (dataframe)
    :param: bought_time (date.time)
    :return float32
    """
    if df is not None:
        bought_time_str = bought_time.strftime("%H:%M:%S")
        mask = (df['date'].values >= bought_time_str)
        #price = df[mask]['high'].max()
        #print("max price after bought= {price}")
        return df[mask]['high'].max()


def convert(s):
    """
    convert the time format from str to date.time
    :param: s (str ,YYYY-MM-DD)
    :return date.time
    """
    return datetime.datetime.strptime(s, '%Y-%m-%d')


def exceed_15h():
    """
    check the current time has exceed 15:00
    :return True or False
    """
    if datetime.datetime.now().hour >= STP_BUY_HR:
        return True
    else:
        return False

if __name__ == "__main__":
    market_opened(TIME_ZONE_ADJ,2)
