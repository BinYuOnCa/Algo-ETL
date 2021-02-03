import pandas as pd
import logging

from util.config import global_config
import lib.db_wrap
import lib.etl
import util.log
import util.error
import lib.twilio_wrap

stop_counter_if_same_errors = global_config['etl']['stop_counter_if_same_errors']

conn = lib.db_wrap.get_connection()

# Get selected symbol list from csv file
symbols_df = pd.read_csv(global_config['etl']['symbols_file'], header=None, names=['symbol'])
symbols_df = symbols_df.sample(frac=1)  # reshuffle
symbols_df['result_day'] = 'pending'
symbols_df['result_day_err_msg'] = ''
symbols_df['result_min'] = 'pending'
symbols_df['result_min_err_msg'] = ''

# load data
@util.error.on_error(util.error.get_error_handler(wait_time=30, raise_if_same_errors=10))
def apply_func_day_candle(row):
    logging.info(f'>>>>day>>>> {row["symbol"]}')
    try:
        lib.etl.load_day_candle(conn, row['symbol'])
    except Exception as e:
        row['result_day'] = 'error'
        row['result_day_err_msg'] = str(e)
        raise e
    else:
        row['result_day'] = 'ok'


@util.error.on_error(util.error.get_error_handler(wait_time=30, raise_if_same_errors=10))
def apply_func_min_candle(row):
    logging.info(f'>>>>min>>>> {row["symbol"]}')
    try:
        lib.etl.load_1min_candle(conn, row['symbol'])
    except Exception as e:
        row['result_min'] = 'error'
        row['result_min_err_msg'] = str(e)
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
    logging.critical(f"Batch load failed! day({stat_day.get('error', 0)}error, {stat_day.get('pending', 0)}pending, {n_total}total), \
        min({stat_min.get('error', 0)}error, {stat_min.get('pending', 0)}pending, {n_total}total)")
    logging.exception(e)
    raise e
else:
    lib.twilio_wrap.send_sms('Batch load is ok')
