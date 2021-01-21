import pandas as pd
import logging

from util.config import global_config
import lib.db_wrap
import lib.etl
import util.log

conn = lib.db_wrap.get_engine()

# Get selected symbol list from csv file
symbols_df = pd.read_csv(global_config['us_selected_symbols_file'], header=None, names=['symbol'])

# load data
def apply_func(row):
    logging.info(f'>>>> {row["symbol"]}')
    lib.etl.load_day_candle(conn, row['symbol'])


symbols_df.apply(apply_func, axis=1)
