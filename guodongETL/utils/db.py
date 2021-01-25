import math
import os
import sys
import time
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import psycopg2
import pytz

from utils.notice import send_sms, send_email
from utils.settings import DEV_DB_CONFIG, US_EQUITY_1M_FINN_TABLE, US_EQUITY_DAILY_FINN_TABLE, EMAIL_ACCOUNT
from utils.utiltools import get_unix_time_from_datetime, get_time_from_unix_timestamp, get_date_from_unix_timestamp, \
    get_data_via_finhub_lib, get_logger, get_data_via_finhub_request, get_splits_status_via_finhub_request


def connect(params_dic, logger):
    """
    Connect to the PostgreSQL database server
    :param params_dic:  database connection params
    :return: db instance connection
    """
    conn = None
    try:
        # connect to the PostgreSQL server
        logger.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
        sys.exit(1)
    logger.info("Connection successful")
    return conn


def get_most_recent_date(conn, table_name, symbol: str):
    cursor = conn.cursor()
    get_time_sql = f"select date_key_int from {table_name} where symbol='{symbol}' order by date_key_int desc limit 1;"
    cursor.execute(get_time_sql)
    most_recent_date = cursor.fetchone()
    return most_recent_date[0] if most_recent_date is not None else None


def remove_duplicate_data(conn, table_name):
    cursor = conn.cursor()
    if table_name == US_EQUITY_1M_FINN_TABLE:
        remove_duplicate_sql = f"delete from {table_name} a " \
                               f"where a.ctid = any (array(select ctid from " \
                               f"(select row_number() over (partition by symbol,date_key_int,time_stamp), ctid from" \
                               f" {table_name}) t where t.row_number > 1));"
    elif table_name == US_EQUITY_DAILY_FINN_TABLE:
        remove_duplicate_sql = f"delete from {table_name} a " \
                               f"where a.ctid = any (array(select ctid from " \
                               f"(select row_number() over (partition by symbol,date_key_int), ctid from" \
                               f" {table_name}) t where t.row_number > 1));"
    cursor.execute(remove_duplicate_sql)
    cursor.close()


def delete_symbol_data(conn, table_name, symbol: str):
    cursor = conn.cursor()
    delete_symbol_sql = f"delete from {table_name} where symbol='{symbol}';"
    cursor.execute(delete_symbol_sql)


def copy_from_file(conn, df, table, logger):
    """
    save the dataframe on disk as a csv file, load the csv file
    and use copy_from() to copy it to the table
    """
    # Save the dataframe to disk
    tmp_df = "./tmp_dataframe.csv"
    df.to_csv(tmp_df, index=False, header=False)
    f = open(tmp_df, 'r')
    cursor = conn.cursor()
    try:
        cursor.copy_from(f, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        f.close()
        os.remove(tmp_df)
        logger.error("Error: %s", error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
    f.close()
    os.remove(tmp_df)


def copy_from_stringio(conn, df, table, logger):
    """
    save the dataframe in memory
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)

    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error("Error: %s", error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()


def has_splits(symbol, db_most_recent_date):
    if db_most_recent_date is None:
        return False
    return get_splits_status_via_finhub_request(symbol, db_most_recent_date)


def get_candles_data(conn, symbol: str, resolution: str, table_name, logger, start_unix_time=None, end_unix_time=None):
    try:
        db_most_recent_date = get_most_recent_date(conn, table_name, symbol)
        logger.info(f"db_most_recent_date: {db_most_recent_date} for {symbol}")

        if has_splits(symbol, db_most_recent_date):
            logger.warning(f"has splits for {symbol}")
            start_unix_time = 0
            delete_symbol_data(conn, US_EQUITY_DAILY_FINN_TABLE, symbol)
            delete_symbol_data(conn, US_EQUITY_1M_FINN_TABLE, symbol)
        if db_most_recent_date is None:
            start_unix_time = 0 if start_unix_time is None else start_unix_time
        else:
            start_unix_time = get_unix_time_from_datetime(db_most_recent_date,
                                                          "23:59:59") if start_unix_time is None else start_unix_time

        est = pytz.timezone('US/Eastern')
        now = datetime.now().astimezone(est).replace(tzinfo=None)
        end_unix_time = get_unix_time_from_datetime(now.strftime('%Y%m%d'),
                                                    now.strftime(
                                                        '%H:%M:%S')) if end_unix_time is None else end_unix_time

        start_date = datetime.utcfromtimestamp(start_unix_time).strftime('%Y%m%d')
        end_date = datetime.utcfromtimestamp(end_unix_time).strftime('%Y%m%d')

        if (resolution == "D" and start_date != end_date) or end_unix_time - start_unix_time < 60 * 60 * 24 * 30:
            res = get_data_via_finhub_lib(symbol, resolution, start_unix_time, end_unix_time)
            df = df_process(res, resolution, start_unix_time, end_unix_time, symbol, logger)
            if df is not None:
                copy_from_file(conn, df, table_name, logger)  # copy the dataframe to SQL
                logger.info(
                    f"get and save {len(df)} rows {resolution} data candles from "
                    f"{datetime.utcfromtimestamp(start_unix_time).strftime('%Y-%m-%d %H:%M:%S')} "
                    f"to {datetime.utcfromtimestamp(end_unix_time).strftime('%Y-%m-%d %H:%M:%S')} for {symbol}.")
            time.sleep(0.5)

        else:
            start_unix_time_for_1m = end_unix_time - (60 * 60 * 24 * 365)
            month = math.ceil((end_unix_time - start_unix_time_for_1m) / (60 * 60 * 24 * 30))
            start_date = datetime.strptime(get_date_from_unix_timestamp(start_unix_time_for_1m), "%Y%m%d")
            logger.info(f"{start_date} for 1m for {symbol}")
            logger.info(f"{month} month for 1m for {symbol}")

            for i in range(0, month):
                end_date = start_date + timedelta(weeks=4)
                inner_start_unix_time = get_unix_time_from_datetime(start_date.strftime('%Y%m%d'), "23:59:59")
                inner_end_unix_time = get_unix_time_from_datetime(end_date.strftime('%Y%m%d'), "23:59:59")
                if inner_end_unix_time < end_unix_time:
                    res = get_data_via_finhub_request(symbol, resolution, inner_start_unix_time, inner_end_unix_time)
                    df = df_process(res, resolution, inner_start_unix_time, inner_end_unix_time, symbol, logger)
                    if df is not None:
                        copy_from_file(conn, df, table_name, logger)  # copy the dataframe to SQL
                        logger.info(
                            f"get and save {len(df)} rows 1m candles from "
                            f"{datetime.utcfromtimestamp(inner_start_unix_time).strftime('%Y-%m-%d')} "
                            f"to {datetime.utcfromtimestamp(inner_end_unix_time).strftime('%Y-%m-%d')} for {symbol}.")
                    start_date = end_date
                    time.sleep(0.5)

    except Exception as e:
        logger.error(e)


def df_process(res, resolution, start_unix_time, end_unix_time, symbol, logger):
    if res is not None and res["s"] == "ok":
        df = pd.DataFrame(data=res)
        df.loc[:, 'symbol'] = symbol.strip()
        df = df.rename(columns={
            "c": "close_price",
            "h": "high_price",
            "l": "low_price",
            "o": "open_price",
            "v": "volume",
        })
        if resolution == "D":
            df["date_key_int"] = df["t"].apply(lambda x: get_date_from_unix_timestamp(x))
            df = df[["symbol", "date_key_int", "close_price", "high_price", "low_price", "open_price", "volume"
                     ]]
        else:
            df["time_stamp"] = df["t"].apply(lambda x: get_time_from_unix_timestamp(x))
            df["date_key_int"] = df["t"].apply(lambda x: get_date_from_unix_timestamp(x))
            df = df[
                ["symbol", "date_key_int", "close_price", "high_price", "low_price", "open_price", "volume",
                 "time_stamp"]]

        return df if df.size != 0 else None
    else:
        logger.warning(
            f"{res['s']} from {datetime.utcfromtimestamp(start_unix_time).strftime('%Y-%m-%d')} "
            f"to {datetime.utcfromtimestamp(end_unix_time).strftime('%Y-%m-%d')} for {symbol}.")
        return None
