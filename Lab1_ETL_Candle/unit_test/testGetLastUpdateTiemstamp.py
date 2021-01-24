from utils.dateTimeHelper import getNextStartingMinute, getLastUpdateTS_int, getNextStartingDay
from utils.dbHelper import get_db_connect, get_next_start_id, connect, getLastUpdateTimeStamp
from datetime import datetime

conn = get_db_connect()
lastDailyTime = getLastUpdateTimeStamp(conn, "us_equity_daily_finn", "AAPL")
print(lastDailyTime)


lastint = getLastUpdateTS_int("us_equity_daily_finn", "AAPL")

print(datetime.utcfromtimestamp(lastint).strftime('%Y-%m-%d %H:%M:%S'))

nextmintues = getNextStartingMinute("us_equity_1m_finn", "AAPL")

print(datetime.utcfromtimestamp(nextmintues).strftime('%Y-%m-%d %H:%M:%S'))


nextday = getNextStartingDay("us_equity_daily_finn", "AAPL")
print(datetime.utcfromtimestamp(nextday).strftime('%Y-%m-%d %H:%M:%S'))
