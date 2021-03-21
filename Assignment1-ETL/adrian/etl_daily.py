import finnhub
import pandas as pd
import datetime
import os
import time
from time import sleep
from utils import db_util
from utils.finn_util import get_stock_candle
from utils.finn_util import format_candle_df
from utils.time_util import usest_str_from_ts
from classes.notice import Notice


def load_and_etl(conn, symbol, table, start_ts, end_ts, res='D'):
    '''
    '''
    try:
        df = get_stock_candle(symbol, res=res, from_ts=start_ts, to_ts=end_ts)
        start_dt = usest_str_from_ts(start_ts)
        end_dt = usest_str_from_ts(end_ts)
        period_str = f'from: {start_dt} ({start_ts}), to: {end_dt} ({end_ts})'
        if (df is not None and df['t'].max() > start_ts):
            df = df[df['t'] > start_ts]
            formatted_df = format_candle_df(df, symbol)
            db_util.copy_from_csv(conn, formatted_df, table)
            print(f'Retrieved: stock: {symbol} {period_str} resolution: {res} to table: {table} total records: {formatted_df.shape[0]}')
        else:
            print(f'No data in request range of the stock {symbol}, {period_str}')
    except Exception as e:
        err_msg = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'    Failed when requesting candle data for '+symbol
        print(err_msg+'\n'+str(e))
        with open('error.log', 'a') as f:
            f.write(err_msg+'\n'+str(e)+'\n')


def send_email_notice():
    '''
    send email notification and errolog if exists
    '''
    email_notice = Notice('Task done successfully.')
    if os.path.exists('error.log'):
        logfile = 'error.log'
        email_notice.content_type = 'ETL Warning!'
        email_notice.content = 'Error(s) occurred in ETL. Please see the log file for details.'
    else:
        logfile = None
    msg = email_notice.email_msg(logfile=logfile)
    email_notice.send_email(msg)


if __name__ == '__main__':
    if os.path.exists('error.log'): os.remove('error.log')
    # Read stock symbols in
    symbol_file = 'stock_symbol.csv'
    symbol_list = pd.read_csv(symbol_file)['symbol'].to_list()

    # Create postgresql connection and cursor
    conn = db_util.connect_to_db()
    if conn is not None:
        table_list = [{'table_name':'us_stock_daily','res':'D'},
                      {'table_name':'us_stock_1m','res':'1'}]
        for table in table_list:
            now = int(datetime.datetime.now().timestamp())
            init_time = time.time()
            for i, symbol in enumerate(symbol_list):
                # prevent requests from exceeding limit (60 queries per minute)
                if not (i + 1) % 60:
                    sleeptime = init_time + 61 - time.time()  # give 1 seconds buffer
                    if sleeptime > 0:
                        print(f'Too frequent request. Sleep {sleeptime} seconds to prevent server denial.')
                        time.sleep(sleeptime)
                    init_time = time.time()
                #processing code
                start_ts = db_util.get_last_ts(conn, table['table_name'], symbol)
                load_and_etl(conn, symbol, table['table_name'], start_ts, end_ts=now, res=table['res'])
            sleep(60)  # rest 1 minute after each table is finished
        conn.close()
    send_email_notice()

