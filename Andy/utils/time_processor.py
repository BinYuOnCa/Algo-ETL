from .db import Database
from .config import tab_names, day_to_sec, cred_info
from datetime import datetime, timedelta
import time
import pandas as pd


def add_days(date, increment):
    '''
    returns a new datetime obj after adding increment days to date
    :param date: datetime obj
    :param increment: (Int)
    :return: datetime obj
    '''
    date = datetime.strptime(str(date), '%Y%m%d')
    return date + timedelta(days=increment)


def timeframe_gen(start, end, n=None):
    '''
    returns a unix timeframe generator from start to end with n as increment
    :param start: datetime obj
    :param: end: datetime obj 
    :param: n: (Int or None)
    :return: (generator of (dict Str Int))
    '''
    end = end + timedelta(days=1, seconds=-1)
    while start < end:
        cutoff = min(start + timedelta(days=n, seconds=-1),
                     end) if n is not None else end
        yield {
            'start': int(datetime.timestamp(start)),
            'end': int(datetime.timestamp(cutoff))
        }
        start = cutoff + timedelta(seconds=1)


def convert_from_sec(s):
    '''
    returns a string of time spent based on seconds s
    :param s: (Int)
    :return: (Str)
    '''
    beyond_day = s // day_to_sec
    within_day = s % day_to_sec
    t = time.gmtime(within_day)
    days = '' if not beyond_day else '{:02} day, '.format(s // day_to_sec)
    res = days + time.strftime('%H hr, %M min, %S sec', t)
    return res


def export_recent_dates(interval, conn_str, csv_file='recent_dates.csv'):
    '''
    writes a csv file which records the most recent date in the tables from rds
    :param interval: 1m or 1d
    :param conn_str: (Str)
    :param csv_file : (Str)
    :return
    '''
    sql = '''
    SELECT symbol, MAX(date_int_key) AS recent_date FROM {}
    GROUP BY symbol
    '''.format(tab_names[interval])
    with Database(conn_str) as conn:
        df = pd.read_sql(sql, conn.connector)
    df.to_csv(interval + '_' + csv_file, index=False)


def get_rencent_dates(interval):
    '''
    reads recent date file and transforms it into a dict
    :param interval: 1m or 1d
    :return: (dict Str Int)
    '''
    recent_dates = pd.read_csv(interval + '_recent_dates.csv')
    res_dict = recent_dates.set_index('symbol').T.to_dict('list')
    return res_dict
