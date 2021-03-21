import pandas as pd
import logging

from util.config import global_config
import lib.db_wrap
import lib.etl
import util.log
import util.error

stop_counter_if_same_errors = global_config['etl']['stop_counter_if_same_errors']

conn = lib.db_wrap.get_connection()

# Get selected symbol list from csv file
symbols_df = pd.read_csv(global_config['etl']['symbols_file'], header=None, names=['symbol'])
symbols_df = symbols_df.sample(frac=1)
symbols_df['result_day'] = 'pending'
symbols_df['result_day_err_msg'] = ''
symbols_df['result_min'] = 'pending'
symbols_df['result_min_err_msg'] = ''

lib.etl.trunc_all_tables()

# load data
@util.error.on_error(util.error.get_error_handler(wait_time=30, raise_if_same_errors=10))
def apply_func_day_candle(row):
    logging.info(f'>>>>day>>>> {row["symbol"]}')
    try:
        lib.etl.load_full_day_candle_from_finnhub(conn, row['symbol'], delete_before_load=False)
    except Exception as e:
        row['result_day'] = 'error'
        row['result_day_err_msg'] = f'error:{e}'
        raise e
    else:
        row['result_day'] = 'ok'


@util.error.on_error(util.error.get_error_handler(wait_time=30, raise_if_same_errors=10))
def apply_func_min_candle(row):
    logging.info(f'>>>>min>>>> {row["symbol"]}')
    try:
        lib.etl.load_full_min_candle_from_finnhub(conn, row['symbol'], delete_before_load=False)
    except Exception as e:
        row['result_min'] = 'error'
        row['result_min_err_msg'] = f'error:{e}'
        raise e
    else:
        row['result_min'] = 'ok'


try:
    symbols_df.apply(apply_func_day_candle, axis=1)
    symbols_df.apply(apply_func_min_candle, axis=1)
except Exception as e:
    n_total = len(symbols_df)
    stat_day = symbols_df['result_day'].value_counts()
    stat_min = symbols_df['result_min'].value_counts()
    logging.critical(f"Full load failed! day({stat_day.get('error', 0)}error, {stat_day.get('pending', 0)}pending, {n_total}total), \
                        min({stat_min.get('error', 0)}error, {stat_min.get('pending', 0)}pending, {n_total}total)")
    logging.exception(e)
    raise e
else:
    lib.twilio_wrap.send_sms('Full load is ok')


# def _adjust_day_candles_relative_to_20210101(day_candles_df, splits_df):
#     '''
#     Adjust price and volumn to the value relative to 20210101

#     day_candles_df - day_candle for one ticker
#     splits_df - splits of one ticker
#     '''
#     day_candles_df['split_factor'] = 1
#     day_candles_last_date = day_candles_df['t'].max()
#     for _, symbol, date, fromFactor, toFactor in splits_df.itertuples():
#         if date <= day_candles_last_date:  # ignore the splits in the future
#             day_candles_df.loc[day_candles_df['t'] < date, 'split_factor'] *= toFactor / fromFactor
#     day_candles_df_before_20210101 = day_candles_df[day_candles_df['t'] < datetime.datetime(2021, 1, 1)].sort_values('t')
#     split_factor_20210101 = day_candles_df_before_20210101.iloc[-1]['split_factor'] if not day_candles_df_before_20210101.empty else 1
#     if split_factor_20210101 != 1:
#         day_candles_df['split_factor'] /= split_factor_20210101
#         day_candles_df['o'] *= split_factor_20210101
#         day_candles_df['h'] *= split_factor_20210101
#         day_candles_df['l'] *= split_factor_20210101
#         day_candles_df['c'] *= split_factor_20210101
#         day_candles_df['v'] /= split_factor_20210101


# def _adjust_minute_candles_relative_to_20210101(minute_candles_df, splits_df):
#     '''
#     Adjust price and volumn to the value relative to 20210101

#     minute_candles_df - minute_candle for one ticker
#     splits_df - splits of one ticker
#     '''
#     minute_candles_df['split_factor'] = 1
#     for _, symbol, date, fromFactor, toFactor in splits_df.itertuples():
#         minute_candles_df.loc[minute_candles_df['t'] < date, 'split_factor'] *= toFactor / fromFactor
#     minute_candles_df_before_20210101 = minute_candles_df[minute_candles_df['t'] < datetime.datetime(2021, 1, 1)].sort_values('t')
#     split_factor_20210101 = minute_candles_df_before_20210101.iloc[-1]['split_factor'] if not minute_candles_df_before_20210101.empty else 1
#     if split_factor_20210101 != 1:
#         minute_candles_df['split_factor'] /= split_factor_20210101
#     minute_candles_df['o'] /= minute_candles_df['split_factor']
#     minute_candles_df['h'] /= minute_candles_df['split_factor']
#     minute_candles_df['l'] /= minute_candles_df['split_factor']
#     minute_candles_df['c'] /= minute_candles_df['split_factor']
#     minute_candles_df['v'] *= minute_candles_df['split_factor']

# def clear_data_by_ticker(symbol, con):
#     pass

# def full_load_data_of_a_ticker(symbol, con, remove_data_first=True):
#     # Remove data first
#     if remove_data_first:
#         clear_data_by_ticker(symbol, con)

#     # get split data from finnhub by symbol
#     splits = finnhub_wrap.get_split(symbol)

#     # save to finnhub_us_selected_split
#     splits.to_sql(table_name_finnhub_us_selected_split, con=con, if_exists='append', index=False, method='multi')

#     # get day candle from finnhub by symbol
#     day_candles = finnhub_wrap.get_stock_day_candles(symbol)

#     # save to finnhub_us_selected_day_candle
#     day_candles.to_sql(table_name_finnhub_us_selected_day_candle, con=con, if_exists='append', index=False, method='multi')

#     # add column 'split_factor', and adjust the value relative to 20210101
#     # Day candle data from finnhub is already adjusted to toady
#     _adjust_day_candles_relative_to_20210101(day_candles, splits[splits['symbol'] == symbol])

#     # save to adjusted us_selected_day_candle
#     day_candles_column_name_map = {'t': 'date', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'vol'}
#     day_candles.rename(columns=day_candles_column_name_map, inplace=True)
#     day_candles.to_sql(table_name_us_selected_day_candle, con=con, if_exists='append', index=False, method='multi')

#     # get 1min candle from finnhub by symbol
#     minute_candles = finnhub_wrap.get_stock_1min_candles(symbol)

#     # save to finnhub_us_selected_1min_candle
#     minute_candles.to_sql(table_name_finnhub_us_selected_1min_candle, con=con, if_exists='append', index=False, method='multi')

#     # add column 'split_factor', and adjust the value relative to 20210101
#     # 1min candle data from finnhub is not adjusted according to splits
#     _adjust_minute_candles_relative_to_20210101(minute_candles, splits[splits['symbol'] == symbol])

#     # save to adjusted us_selected_1min_candle with split_factor
#     minute_candles_column_name_map = {'t': 'time', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'vol'}
#     minute_candles.rename(columns=minute_candles_column_name_map, inplace=True)
#     minute_candles.to_sql(table_name_us_selected_1min_candle, con=con, if_exists='append', index=False, method='multi')


# # iter for all symbols
# def full_reload_all_data(on_error=None):
#     '''
#     symbols - iterable or None (default is to read from default selected symbol list file)
#     on_error - trigger to run on_error(e)
#     '''
#     trunc_all_tables()
#     symbols = get_selected_symbols_from_file()
#     for i, symbol in enumerate(symbols):
#         print('>>>>', i, symbol)
#         try:
#             with get_connection().begin() as conn:
#                 full_load_data_of_a_ticker(symbol, conn, remove_data_first=False)
#         except Exception as e:
#             if on_error is None:
#                 if isinstance(e, SQLAlchemyError):
#                     err_msg = e.__dict__['orig']
#                 else:
#                     err_msg = str(e)
#                 print('====', symbol, err_msg)
#             else:
#                 on_error(e)
