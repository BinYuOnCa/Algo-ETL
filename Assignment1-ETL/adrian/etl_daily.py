import finnhub
import pandas as pd
import datetime
import os
from time import sleep
from utils import db_util
from utils.finn_util import get_stock_candle
from utils.finn_util import format_candle_df
from classes.notice import Notice


def finn_requests_etl(conn, symbol_list, table_name, res='D', start_ts='db_recent_ts', end_ts='now', drop_duplicate=False):
    '''
    Use requests module to retrieve candle data and copy to db.
    '''
    if end_ts == 'now':
        end_ts = int(datetime.datetime.now().timestamp())
    for symbol in symbol_list:
        if start_ts == 'db_recent_ts':
            start_ts = db_util.get_last_ts(conn, table_name, symbol)
        try:
            df = get_stock_candle(symbol, res, start_ts, end_ts)
            if df is None:
                print(f'No data in request range of the stock {symbol}, from: {start_ts} to: {end_ts}')
                err_msg = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'    '+'No data retrieved for '+symbol
                with open('error.log', 'a') as f:
                    f.write(err_msg+'\n')
            else:
                if drop_duplicate:
                    df = df[df['t'] > start_ts]
                if not df.empty:
                    # print(df)
                    formatted_df = format_candle_df(df, symbol)
                    db_util.copy_from_csv(conn, formatted_df, table_name)
                    print(f'retrieved: stock: {symbol} from: {start_ts} to: {end_ts} resolution: {res} to table: {table_name} total records: {formatted_df.shape[0]}')
                else:
                    print(f'No data in request range of the stock {symbol}, from: {start_ts} to: {end_ts}')
                    err_msg = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'    '+'No data retrieved for '+symbol
                    with open('error.log', 'a') as f:
                        f.write(err_msg+'\n')
        except Exception as e:
            err_msg = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'    Failed when requesting candle data for '+symbol
            print(err_msg+'\n'+str(e))
            with open('error.log', 'a') as f:
                f.write(err_msg+'\n'+str(e)+'\n')
        sleep(1.5)


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
    if os.path.exists('error.log'): os.remove('error.log')



# Read stock symbols in
symbol_file = 'stock_symbol.csv'
symbol_list = pd.read_csv(symbol_file)['symbol'].to_list()

# Create postgresql connection and cursor
print('Connecting to db...')
conn = db_util.connect_to_db()
if conn is not None:
    end_ts = 'now'
    finn_requests_etl(conn, symbol_list, 'us_stock_daily', end_ts=end_ts, drop_duplicate=True)
    finn_requests_etl(conn, symbol_list, 'us_stock_1m', res='1', end_ts=end_ts, drop_duplicate=True)
    conn.close()
send_email_notice()

