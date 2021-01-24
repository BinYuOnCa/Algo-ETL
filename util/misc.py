import pandas as pd
import time

def get_now_datetime(tzname=None) -> str:
    ''' 返回统一格式的string
    '''
    if tzname is None:
        tzname = time.tzname[0]
    return pd.Timestamp.now(tzname).strftime('%Y-%m-%d %H:%M:%S')
