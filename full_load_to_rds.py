import pandas as pd
import datetime
from sqlalchemy.exc import SQLAlchemyError

from util.config import global_config
from lib.db_wrap import truncate, get_engine
import lib.finnhub_wrap as finnhub_wrap
from lib.etl import get_selected_symbols_from_file

table_name_finnhub_us_selected_day_candle = 'finnhub_us_selected_day_candle'
table_name_finnhub_us_selected_1min_candle = 'finnhub_us_selected_1min_candle'
table_name_us_selected_day_candle = 'us_selected_day_candle'
table_name_us_selected_1min_candle = 'us_selected_1min_candle'
table_name_finnhub_us_selected_split = 'finnhub_us_selected_split'

def _adjust_day_candles_relative_to_20210101(day_candles_df, splits_df):
    '''
    Adjust price and volumn to the value relative to 20210101

    day_candles_df - day_candle for one ticker
    splits_df - splits of one ticker
    '''
    day_candles_df['split_factor'] = 1
    day_candles_last_date = day_candles_df['t'].max()
    for _, symbol, date, fromFactor, toFactor in splits_df.itertuples():
        if date <= day_candles_last_date:  # ignore the splits in the future
            day_candles_df.loc[day_candles_df['t'] < date, 'split_factor'] *= toFactor / fromFactor
    day_candles_df_before_20210101 = day_candles_df[day_candles_df['t'] < datetime.datetime(2021, 1, 1)].sort_values('t')
    split_factor_20210101 = day_candles_df_before_20210101.iloc[-1]['split_factor'] if not day_candles_df_before_20210101.empty else 1
    if split_factor_20210101 != 1:
        day_candles_df['split_factor'] /= split_factor_20210101
        day_candles_df['o'] *= split_factor_20210101
        day_candles_df['h'] *= split_factor_20210101
        day_candles_df['l'] *= split_factor_20210101
        day_candles_df['c'] *= split_factor_20210101
        day_candles_df['v'] /= split_factor_20210101


def _adjust_minute_candles_relative_to_20210101(minute_candles_df, splits_df):
    '''
    Adjust price and volumn to the value relative to 20210101

    minute_candles_df - minute_candle for one ticker
    splits_df - splits of one ticker
    '''
    minute_candles_df['split_factor'] = 1
    for _, symbol, date, fromFactor, toFactor in splits_df.itertuples():
        minute_candles_df.loc[minute_candles_df['t'] < date, 'split_factor'] *= toFactor / fromFactor
    minute_candles_df_before_20210101 = minute_candles_df[minute_candles_df['t'] < datetime.datetime(2021, 1, 1)].sort_values('t')
    split_factor_20210101 = minute_candles_df_before_20210101.iloc[-1]['split_factor'] if not minute_candles_df_before_20210101.empty else 1
    if split_factor_20210101 != 1:
        minute_candles_df['split_factor'] /= split_factor_20210101
    minute_candles_df['o'] /= minute_candles_df['split_factor']
    minute_candles_df['h'] /= minute_candles_df['split_factor']
    minute_candles_df['l'] /= minute_candles_df['split_factor']
    minute_candles_df['c'] /= minute_candles_df['split_factor']
    minute_candles_df['v'] *= minute_candles_df['split_factor']




def trunc_all_tables():
    truncate(table_name_finnhub_us_selected_day_candle)
    truncate(table_name_finnhub_us_selected_1min_candle)
    truncate(table_name_us_selected_day_candle)
    truncate(table_name_us_selected_1min_candle)
    truncate(table_name_finnhub_us_selected_split)


def clear_data_by_ticker(symbol, con):
    pass


def full_load_data_of_a_ticker(symbol, con, remove_data_first=True):
    # Remove data first
    if remove_data_first:
        clear_data_by_ticker(symbol, con)

    # get split data from finnhub by symbol
    splits = finnhub_wrap.get_split(symbol)

    # save to finnhub_us_selected_split
    splits.to_sql(table_name_finnhub_us_selected_split, con=con, if_exists='append', index=False, method='multi')

    # get day candle from finnhub by symbol
    day_candles = finnhub_wrap.get_stock_day_candles(symbol)

    # save to finnhub_us_selected_day_candle
    day_candles.to_sql(table_name_finnhub_us_selected_day_candle, con=con, if_exists='append', index=False, method='multi')

    # add column 'split_factor', and adjust the value relative to 20210101
    # Day candle data from finnhub is already adjusted to toady
    _adjust_day_candles_relative_to_20210101(day_candles, splits[splits['symbol'] == symbol])

    # save to adjusted us_selected_day_candle
    day_candles_column_name_map = {'t': 'date', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'vol'}
    day_candles.rename(columns=day_candles_column_name_map, inplace=True)
    day_candles.to_sql(table_name_us_selected_day_candle, con=con, if_exists='append', index=False, method='multi')

    # get 1min candle from finnhub by symbol
    minute_candles = finnhub_wrap.get_stock_1min_candles(symbol)

    # save to finnhub_us_selected_1min_candle
    minute_candles.to_sql(table_name_finnhub_us_selected_1min_candle, con=con, if_exists='append', index=False, method='multi')

    # add column 'split_factor', and adjust the value relative to 20210101
    # 1min candle data from finnhub is not adjusted according to splits
    _adjust_minute_candles_relative_to_20210101(minute_candles, splits[splits['symbol'] == symbol])

    # save to adjusted us_selected_1min_candle with split_factor
    minute_candles_column_name_map = {'t': 'time', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'vol'}
    minute_candles.rename(columns=minute_candles_column_name_map, inplace=True)
    minute_candles.to_sql(table_name_us_selected_1min_candle, con=con, if_exists='append', index=False, method='multi')


# iter for all symbols
def full_reload_all_data(on_error=None):
    '''
    symbols - iterable or None (default is to read from default selected symbol list file)
    on_error - trigger to run on_error(e)
    '''
    trunc_all_tables()
    symbols = get_selected_symbols_from_file()
    for i, symbol in enumerate(symbols):
        print('>>>>', i, symbol)
        try:
            with get_engine().begin() as conn:
                full_load_data_of_a_ticker(symbol, conn, remove_data_first=False)
        except Exception as e:
            if on_error is None:
                if isinstance(e, SQLAlchemyError):
                    err_msg = e.__dict__['orig']
                else:
                    err_msg = str(e)
                print('====', symbol, err_msg)
            else:
                on_error(e)


if __name__ == '__main__':
    full_reload_all_data()
