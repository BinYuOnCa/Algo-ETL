from utils.db import Database
from utils.config import *
import pandas as pd

with Database(cred_info['conn_str']) as conn:
    sql = '''
    select symbol, date_int_key, timestamp from us_equity_1m_finn
    group by symbol, date_int_key, timestamp
    having count(symbol) > 1
    '''
    df = pd.read_sql(sql,conn.connector)
    df.to_csv('check.csv',index=False)
    sql = '''
    SELECT symbol from us_equity_1m_finn
    group by symbol
    '''
    df = pd.read_sql(sql,conn.connector)
    df.to_csv('check2.csv',index=False)

