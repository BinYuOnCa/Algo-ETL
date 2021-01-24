from utils.dbHelper import get_db_connect, getLastUpdateTimeStamp
from datetime import datetime

import datetime as dt


def getLastUpdateTS_int(table, ticker):
    conn = get_db_connect()
    lastTimeObj1 = getLastUpdateTimeStamp(conn, table, ticker)
    lastTime_int = int(lastTimeObj1.timestamp())
    return lastTime_int

def getNextStartingMinute(table,ticker):
    conn = get_db_connect()
    lastTimeObj2 = getLastUpdateTimeStamp(conn, table, ticker)
    nextMinute = lastTimeObj2 + dt.timedelta(minutes=1)
    return int(nextMinute.timestamp())

def getNextStartingDay(table,ticker):
    conn = get_db_connect()
    lastTimeObj3 = getLastUpdateTimeStamp(conn, table, ticker)
    nextDay = lastTimeObj3 + dt.timedelta(days=1)
    return int(nextDay.timestamp())


