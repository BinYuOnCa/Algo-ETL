import os
import etl_fn.ETL as ETL
import daily_task.today as end
from utils.time_processor import get_rencent_dates

if os.path.exists('failed_symbols_daily.csv'):
    recent_dates_daily = get_rencent_dates('1d')
    ETL('1d', end_date=end, symbol_file='failed_symbols_daily.csv')

if os.path.exists('failed_symbols_1m.csv'):
    ETL('1d', end_date=end, symbol_file='failed_symbols_1m.csv')
