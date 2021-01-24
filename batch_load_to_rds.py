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

# load data
@util.error.on_error(util.error.get_error_handler(wait_time=30, raise_if_same_errors=10))
def apply_func_day_candle(row):
    logging.info(f'>>>>day>>>> {row["symbol"]}')
    lib.etl.load_day_candle(conn, row['symbol'])


@util.error.on_error(util.error.get_error_handler(wait_time=30, raise_if_same_errors=10))
def apply_func_min_candle(row):
    logging.info(f'>>>>min>>>> {row["symbol"]}')
    lib.etl.load_1min_candle(conn, row['symbol'])


symbols_df.apply(apply_func_day_candle, axis=1)
symbols_df.apply(apply_func_min_candle, axis=1)
