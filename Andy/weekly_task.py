from datetime import datetime
import etl_fn as e
import os
from utils.time_processor import get_rencent_dates
from utils.alert import notification

now = datetime.now()
today = datetime(now.year, now.month, now.day)

notification('Daily candles ETL', 'Starts at {}'.format(datetime.now()))
recent_dates_daily = get_rencent_dates('daily')
e.ETL('daily', end_date=today, recent_dates=recent_dates_daily)

if os.path.exists('failed_symbols_daily.csv'):
    notification('Starts to perform ETL on failed symbols')
    e.ETL('1d', end_date=end, stary_date = recent_dates_daily,
          symbol_file='failed_daily.csv')
                                
                


