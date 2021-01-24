from datetime import datetime
import etl_fn as e
from utils.time_processor import get_rencent_dates

now = datetime.now()
today = datetime(now.year, now.month, now.day)

#recent_dates_daily = get_rencent_dates('1d')
recent_dates_1m = get_rencent_dates('1m')

#e.ETL('1d', end_date=today, recent_dates=recent_dates_daily)
e.ETL('1m', end_date=today, recent_dates=recent_dates_1m)
