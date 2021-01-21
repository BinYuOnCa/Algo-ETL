import pandas as pd

from util.config import global_config
import lib.db_wrap
import lib.etl

conn = lib.db_wrap.get_engine()

# Get selected symbol list from csv file
symbols_df = pd.read_csv(global_config['us_selected_symbols_file'], header=None, names=['symbol'])

# load data
symbols_df.apply(lambda row: lib.etl.load_day_candle(conn, row['symbol']), axis=1)
