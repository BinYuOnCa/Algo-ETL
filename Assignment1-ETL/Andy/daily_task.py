from datetime import datetime
import os
import etl_fn as e
from utils.time_processor import get_rencent_dates
from utils.alert import notification

now = datetime.now()
today = datetime(now.year, now.month, now.day)

notification('Intraday Candles ETL', 'Starts at {}'.format(datetime.now()))
recent_dates_1m = get_rencent_dates('1m')
e.ETL('1m', end_date=today, recent_dates=recent_dates_1m)

if os.path.exists('doc/failed_symbols_1m.csv'):
    notification('Starts to perform ETL on failed symbols')
    e.ETL('1m', end_date=end, stary_date = recent_dates_1m,
          symbol_file='failed_1m.csv')

