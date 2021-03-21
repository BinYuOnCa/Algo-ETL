import psycopg2
from psycopg2 import OperationalError
import os
from io import StringIO
import sys
import pandas as pd
import pandas.io.sql as sqlio
from utils.config import db_config
import datetime
import time


# Define a function that handles and parses psycopg2 exceptions
def log_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    # get the line number when exception occured
    line_n = traceback.tb_lineno
    # connect() error
    s1 = f'psycopg2 ERROR: {err} on line number {line_n}\n'
    s2 = f'psycopg2 traceback: {traceback} -- type: {err_type}\n'
    # psycopg2 extensions.Diagnostics object attribute
    s3 = f'extensions.Diagnostics: {err.diag}\n'
    # print the pgcode and pgerror exceptions
    s4 = f'pgerror: {err.pgerror}\n'
    s5 = f'pgcode: {err.pgcode}\n'
    s_all = s1+s2+s3+s4+s5
    with open('error.log', 'a') as f:
        f.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'    '+s_all)


def connect_to_db():
    conn = None
    for i in range(3):
        print('Connecting to db...')
        try:
            conn = psycopg2.connect(database=db_config['database'],
                                    user=db_config['user'],
                                    password=db_config['password'],
                                    host=db_config['host'],
                                    port=db_config['port'],
                                    connect_timeout=10)
            print('Connect to db successful')
            break
        except OperationalError as e:
            # passing exception to function
            log_psycopg2_exception(e)
            print(f'Connect to db failed. Retrying ({i+1})...')
            # set the connection to 'None' in case of error
            conn = None
    return conn


def copy_from_csv(conn, df, table, columns=None):
    tmp_stream = StringIO()
    df.to_csv(tmp_stream, index=False, header=False)
    tmp_stream.seek(0)
    cur = conn.cursor()
    try:
        cur.copy_from(tmp_stream, table, sep=',', columns=('close', 'high', 'low', 'open', 'timestamp', 'volume','symbol','date_key_int','time_key'))
        conn.commit()
    except OperationalError as e:
        # passing exception to function
        log_psycopg2_exception(e)
    cur.close()
    tmp_stream.close()


def get_last_ts(conn, table, symbol):
    date_column_name = 'timestamp'
    sql_query_last_date = f'''
    SELECT {date_column_name} FROM {table}
    WHERE (symbol = '{symbol}' AND {date_column_name} IS NOT NULL)
    ORDER BY {date_column_name} DESC LIMIT 1
    '''
    cur =  conn.cursor()
    cur.execute(sql_query_last_date)
    last_ts_row = cur.fetchone()
    last_ts = 0
    if last_ts_row is not None:
        last_ts = last_ts_row[0]
    cur.close()
    return last_ts

if __name__ == '__main__':
    conn = connect_to_db()
    last_ts = get_last_ts(conn, 'us_stock_daily', 'TSLA')
    print('---------------')
    print(last_ts)
    print(type(last_ts))
    conn.close()
