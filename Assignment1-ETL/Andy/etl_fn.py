import pandas as pd
import os
from datetime import datetime
from utils.extract import download_candle
from utils.transform import candles_df_to_csv
from utils.db import Database
from utils.memory import memory_using
from utils.config import (cred_info, failed_csv, temp_csv, tab_names, 
                          msg_info, log, mem_lim)
from utils.alert import notification
from utils.time_processor import (export_recent_dates, get_rencent_dates,
                                  convert_from_sec, add_days)


def transfer(df, interval, conn_str):
    '''
    transforms the candles dataframe and uploads to rds
    :param df: pandas df
    :param interval: 1m or 1d
    :param conn_str: (Str)
    :return:
    '''
    if len(df):
        candles_df_to_csv(df, interval)
        with Database(conn_str) as conn:
            conn.load_to_RDS(interval)


def ETL(interval,
        end_date,
        start_date=None,
        symbol_file='doc/symbol.csv',
        memory_limit=mem_lim,
        recent_dates=None,
        conn_str=cred_info['conn_str'],
        silence=False):
    '''
    Perform ETL task
    :param interval: 1m or 1d
    :param end_date: datetime obj
    :param start_date: datetime obj
    :param symbol_file: (Str)
    :param memory_limit: (Int or Float)
    :param recent_dates: ((dict Str Int) or None)
    :param conn_str: (Str)
    :param silence: (Bool)
    :return: 
    '''
    # Read symbol
    with open(symbol_file, 'r') as symbol_f:
        symbols = [s.strip() for s in symbol_f.readlines()]
    # Generate empty df
    clean_candles_df = pd.DataFrame()
    n_records = 0
    failed_symbol = []
    # ETL Begin Time
    time0 = datetime.now()
    # Loop all the symbol
    for symbol in symbols:
        try:
            # Start Extraction
            raw_candles_df = download_candle(symbol=symbol,
                                             start_date=start_date,
                                             end_date=end_date,
                                             interval=interval,
                                             recent_dates=recent_dates)
            clean_candles_df = clean_candles_df.append(raw_candles_df,
                                                       ignore_index=True)
        except Exception as e:
            # 若发生下载问题，跳过此symbol，记载failed的symbol
            log.warning('Failed to download {} - {}'.format(symbol, e))
            failed_symbol.append(symbol + '\n')
        curr_memory = memory_using()
        # Transfer when memory usage is too high or all extraction are done
        if curr_memory > memory_limit or symbol == symbols[-1]:
            n_records += len(clean_candles_df)  # update the number of records
            transfer(clean_candles_df, interval, conn_str)
            clean_candles_df = pd.DataFrame()
    
    if failed_symbol:
        with open('doc/' + failed_csv[interval], 'w') as failed:
            failed.writelines(failed_symbol)

    # ETL End Time
    time1 = datetime.now()

    # Send Notification
    if not silence:
        msg_sent = msg_info['comp'].format(
            tab_names[interval], time0.strftime('%Y-%m-%d %H:%M:%S'),
            time1.strftime('%Y-%m-%d %H:%M:%S'),
            convert_from_sec((time1 - time0).seconds), n_records)
        if failed_symbol:
            with open('doc/' + failed_csv[interval], 'r') as fail:
                failed_sym = fail.readlines()
                msg_sent += msg_info['fail'].format(len(failed_sym),
                                                    ''.join(failed_sym))
        notification(subject='ETL Completion', msg=msg_sent)

    # Export the updated dates
    export_recent_dates(interval, conn_str)

if __name__ == '__main__':
    # Initialize tables
    with Database(cred_info['conn_str']) as conn:
        conn.ddl('''
        CREATE TABLE IF NOT EXISTS us_equity_daily_finn(
            _id SERIAL UNIQUE,
            symbol varchar(25) NOT NULL,
            date_int_key int NOT NULL,
            open_price numeric,
            close_price numeric,
            high_price numeric,
            low_price numeric,
            volume numeric
            );
        CREATE TABLE IF NOT EXISTS us_equity_1m_finn(
            _id SERIAL UNIQUE,
            symbol varchar(25) NOT NULL,
            date_int_key int NOT NULL,
            open_price numeric,
            close_price numeric,
            high_price numeric,
            low_price numeric,
            volume numeric,
            timestamp varchar(8)
            )
            ''')
    # First daily ETL
    start = datetime(2001, 1, 25)
    end = datetime(2021, 1, 22)
    ETL('1d', end_date=end, start_date=start)
    # First 1m ETL
    start = datetime(2020, 1, 25)
    end = datetime(2021, 1, 21)
    ETL('1m', end_date=end, start_date=start)
