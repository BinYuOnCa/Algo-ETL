from datetime import datetime
import pandas as pd
from pathlib import Path

import config.config_parser as conf
import utils.api_timer as api_timer
import utils.convert_time_timestamp as ctt
import utils.db_func as db_func
import utils.finn_func as finn_func
import utils.msg_service as msg_service
import utils.sql_func as sql_func


db_conn = db_func.DB_Conn()
conn = db_conn.create_connection()

def load_candles(ticker, table_name, candle_freq, timer, _conn=conn):
    """
    main function to load candles.
    :param ticker:
    :param table_name:
    :param timer:
    :param _conn:
    :return:
    """
    try:
        isempty = 1
        month_step = 120 if candle_freq == "D" else 1
        last_timestamp = sql_func.get_last_timestamp(ticker, table_name, conn)
        if last_timestamp == None:
            isfirstload = True
            last_close_price = None
            if candle_freq == 1:
                start_timestamp = ctt.convert_datetime_timestamp(
                ctt.get_past_date(_date=datetime.today(), years=1))  # 1 years data
            else:
                start_timestamp = ctt.convert_datetime_timestamp(
                ctt.get_past_date(_date=datetime.today(), years=25))  # 25 years data
        else:
            isfirstload = False
            start_timestamp = last_timestamp
            last_close_price = sql_func.get_symbol_close_price(ticker, table_name)
        stop_timestamp = ctt.convert_datetime_timestamp(datetime.today())
        if start_timestamp + 86400 >= stop_timestamp and candle_freq == "D":
            month_list = ctt.date_by_month_list(
                ctt.convert_timestamp_datetime(start_timestamp),
                ctt.convert_timestamp_datetime(stop_timestamp))
            while month_list.next_timestamp_list(month=month_step) is not None:
                timer.api_timer_handler()
                stock_candle = finn_func.get_candles_df(
                    ticker, candle_freq, month_list.timestamp_list[0], month_list.timestamp_list[1])
                if stock_candle is not None:
                    isempty = isempty and False
                    if isfirstload:
                        sql_func.insert_df_to_db(stock_candle, table_name)
                        isfirstload = False
                    else:
                        sql_func.insert_df_to_db(stock_candle.drop(stock_candle.index[0]), table_name)
                    if candle_freq == "D":
                        db_last_close_price = last_close_price.iloc[0, 0]
                        api_last_close_price = stock_candle.iloc[0, ]["close_price"]
                        issplit = False if api_last_close_price == db_last_close_price else True
                        sql_func.insert_df_to_db(stock_candle.drop(stock_candle.index[0]), table_name)
                        if issplit is True:
                            timer.api_timer_handler()
                            split_df = finn_func.get_split_df(
                                ticker,
                                ctt.convert_timestamp_datetime(month_list.timestamp_list[0]),
                                ctt.convert_timestamp_datetime(month_list.timestamp_list[1]))
                            if split_df is not None:
                                sql_func.insert_df_to_db_iter(split_df)
                else:
                    isempty = isempty and True
        if isempty is True:
            return "no data"
        elif isempty is False:
            return "Success"
        else:
            return "Error"
    except Exception as e:
        try:

            error_sql = "insert into error_rec (error_time, error_info) values ('" \
                        + str(datetime.today()).replace("'", "''") + \
                        "', 'Error when getting data with ticker [" \
                        + str(ticker) + "]: " + str(e).replace("'", "''") + "')"
            with conn.cursor() as cursor:
                cursor.execute(error_sql)
            return str(datetime.today()) \
                   + " error occured when getting candles with '" \
                   + str(ticker) + "', timestampfrom " + str(month_list.timestamp_list[0]) \
                   + "timestampto " + str(month_list.timestamp_list[1]) + " error msg: " + str(e)
        except Exception as e2:
            print('"', datetime.today(),
                  '" , "Error when adding error message to database: ' + str(e2) + '", "'
                  + 'Original Error when getting data with ticker \'', str(ticker), '\': ', e, '"')
            return '"' + str(datetime.today()) + \
                   '" , "Error when adding error message to database: ' + str(e2) + '", "' \
                   + 'Original Error when getting data with ticker \'', str(ticker), '\': ' \
                   + ' timestampfrom ' + str(month_list.timestamp_list[0]) \
                   + "timestampto " + str(month_list.timestamp_list[1]) + " error msg: " + str(e) + '"'
