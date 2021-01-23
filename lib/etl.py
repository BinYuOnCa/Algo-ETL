import pandas as pd
import datetime as dt
import logging

import lib.finnhub_wrap as finn
import lib.db_wrap
import util.error

table_name_us_selected_day_candle_finnhub = 'us_selected_day_candle_finnhub'  # staging
table_name_us_selected_1min_candle_finnhub = 'us_selected_1min_candle_finnhub'  # staging
table_name_us_selected_day_candle = 'us_selected_day_candle'
table_name_us_selected_1min_candle = 'us_selected_1min_candle'
table_name_us_selected_split_finnhub = 'us_selected_split_finnhub'


sql_stmt_last_day_candle = f'''
    WITH last_day_table AS (
        SELECT symbol, MAX(date) AS last_date
        FROM {table_name_us_selected_day_candle}
        GROUP BY symbol
        )
    SELECT all_day_table.symbol, date, open, high, low, close, split_factor
    FROM {table_name_us_selected_day_candle} AS all_day_table
    INNER JOIN last_day_table
    ON all_day_table.symbol = last_day_table.symbol and all_day_table.date = last_day_table.last_date
'''

sql_stmt_last_day_candle_staging = f'''
    WITH last_day_table AS (
        SELECT symbol, MAX(t) AS last_date
        FROM {table_name_us_selected_day_candle_finnhub}
        GROUP BY symbol
        )
    SELECT all_day_table.symbol, t, o, h, l, c
    FROM {table_name_us_selected_day_candle_finnhub} AS all_day_table
    INNER JOIN last_day_table
    ON all_day_table.symbol = last_day_table.symbol and all_day_table.t = last_day_table.last_date
'''

sql_stmt_last_min_candle = f'''
    WITH last_min_table AS (
        SELECT symbol, MAX(time) AS last_min
        FROM {table_name_us_selected_1min_candle}
        GROUP BY symbol
        )
    SELECT all_min_table.symbol, time, open, high, low, close, split_factor
    FROM {table_name_us_selected_1min_candle} AS all_min_table
    INNER JOIN last_min_table
    ON all_min_table.symbol = last_min_table.symbol and all_min_table.time = last_min_table.last_min
'''

sql_stmt_last_min_candle_staging = f'''
    WITH last_min_table AS (
        SELECT symbol, MAX(t) AS last_min
        FROM {table_name_us_selected_1min_candle_finnhub}
        GROUP BY symbol
        )
    SELECT all_min_table.symbol, t, o, h, l, c
    FROM {table_name_us_selected_1min_candle_finnhub} AS all_min_table
    INNER JOIN last_min_table
    ON all_min_table.symbol = last_min_table.symbol and all_min_table.t = last_min_table.last_min
'''

sql_stmt_delete_by_symbol = '''
    DELETE FROM {}
    WHERE symbol = '{}'
'''

sql_stmt_get_split = '''
    SELECT to_factor, from_factor
    FROM {table_name_us_selected_split_finnhub}
    WHERE symbol = '{}' and date = '{}'
'''

_all_symbols_last_day_candle_staging = None
_all_symbols_last_day_candle = None
_all_symbols_last_min_candle_staging = None
_all_symbols_last_min_candle = None

def get_last_day_candle_from_db(conn, from_staging=False):
    sql_stmt = sql_stmt_last_day_candle_staging if from_staging else sql_stmt_last_day_candle
    df = pd.read_sql(sql_stmt, con=conn)
    df.set_index('symbol', inplace=True)
    return df

def get_last_day_candle(symbol=None, from_staging=False):
    '''
    If symbol is None, return last day candles of all symbols
    If symbol is not found, return None
    Last day candles of all symbols are cached after first time access.
    '''
    global _all_symbols_last_day_candle
    global _all_symbols_last_day_candle_staging

    if from_staging and _all_symbols_last_day_candle_staging is None:
        _all_symbols_last_day_candle_staging = get_last_day_candle_from_db(conn=lib.db_wrap.get_engine(), from_staging=True)

    if not from_staging and _all_symbols_last_day_candle is None:
        _all_symbols_last_day_candle = get_last_day_candle_from_db(conn=lib.db_wrap.get_engine(), from_staging=False)

    global_cache = _all_symbols_last_day_candle_staging if from_staging else _all_symbols_last_day_candle

    if symbol is None:
        return global_cache
    else:
        return global_cache.get(symbol, None)  # return None if symbol is not found

def get_last_min_candle_from_db(conn, from_staging=False):
    sql_stmt = sql_stmt_last_min_candle_staging if from_staging else sql_stmt_last_min_candle
    df = pd.read_sql(sql_stmt, con=conn)
    df.set_index('symbol', inplace=True)
    return df

def get_last_min_candle(symbol=None, from_staging=False):
    '''
    If symbol is None, return last minute candles of all symbols
    If symbol is not found, return None
    Last min candles of all symbols are cached after first time access.
    '''
    global _all_symbols_last_min_candle
    global _all_symbols_last_min_candle_staging

    if from_staging and _all_symbols_last_min_candle_staging is None:
        _all_symbols_last_min_candle_staging = get_last_min_candle_from_db(conn=lib.db_wrap.get_engine(), from_staging=True)

    if not from_staging and _all_symbols_last_min_candle is None:
        _all_symbols_last_min_candle = get_last_min_candle_from_db(conn=lib.db_wrap.get_engine(), from_staging=False)

    global_cache = _all_symbols_last_min_candle_staging if from_staging else _all_symbols_last_min_candle

    if symbol is None:
        return global_cache
    else:
        return global_cache.get(symbol, None)  # return None if symbol is not found

def delete_day_candle_in_db(conn, symbol, staging=False):
    table_name = table_name_us_selected_day_candle_finnhub if staging else table_name_us_selected_day_candle
    conn.execute(sql_stmt_delete_by_symbol.format(table_name, symbol))

def delete_min_candle_in_db(conn, symbol, staging=False):
    table_name = table_name_us_selected_1min_candle_finnhub if staging else table_name_us_selected_1min_candle
    conn.execute(sql_stmt_delete_by_symbol.format(table_name, symbol))


def load_full_day_candle_from_finnhub(conn, symbol, delete_before_load=True):
    if delete_before_load:
        delete_day_candle_in_db(conn, symbol, staging=True)
    new_candles = finn.get_stock_day_candles(symbol)
    lib.db_wrap.append_to_db(conn=conn, df=new_candles, table_name=table_name_us_selected_day_candle_finnhub)

def load_new_day_candle_from_finnhub(conn, symbol):
    # Retrive new day candle data from finnhub, include the lastest day in db in order to check split.
    last_day_candle_staging = get_last_day_candle(symbol, from_staging=True)
    last_open_staging, last_date_staging = last_day_candle_staging['open'], last_day_candle_staging['date']
    new_candles = finn.get_stock_day_candles(symbol, start_date=last_date_staging, end_date=dt.datetime.today())

    # Check if there is a split, compared with last day
    f_split = 1
    if (symbol, last_date_staging) not in new_candles.index:
        raise util.error.RemoteHostError(f'Fail to find last day candle from finnhub. Last_date is {last_date_staging}')
    last_open_new = new_candles.loc[(symbol, last_date_staging), 'open']
    if last_open_new != last_open_staging:
        f_split = last_open_staging / last_open_new
    new_candles.drop((symbol, last_date_staging), error='ignore', inplace=True)

    # Save new data to staging db
    lib.db_wrap.append_to_db(conn=conn, df=new_candles, table_name=table_name_us_selected_day_candle_finnhub)

    # Save to primary db
    # TODO


def load_day_candle(conn, symbol):
    '''
    Retrive day candle from finnhub and load to staging and primary db
    '''
    last_day_candle_staging = get_last_day_candle(symbol, from_staging=True)
    if last_day_candle_staging is None or last_day_candle_staging.empty:
        load_full_day_candle_from_finnhub(conn, symbol)
    else:
        load_new_day_candle_from_finnhub(conn, symbol)

def load_full_min_candle_from_finnhub(conn, symbol, delete_before_load=True):
    if delete_before_load:
        delete_min_candle_in_db(conn, symbol, staging=True)
    new_candles = finn.get_stock_1min_candles(symbol)
    lib.db_wrap.append_to_db(conn=conn, df=new_candles, table_name=table_name_us_selected_1min_candle_finnhub, method='copy_from_memory')

def load_new_min_candle_from_finnhub(conn, symbol):
    last_min_candle_staging = get_last_min_candle(symbol, from_staging=True)
    last_min_staging = last_min_candle_staging['t'] + pd.Timedelta('1min') if last_min_candle_staging is None or last_min_candle_staging.empty else None
    new_candles = finn.get_stock_min_candles(symbol, start_time=last_min_staging, end_time=dt.datetime.today().floor('min'))
    # Save new data to staging db
    lib.db_wrap.append_to_db(conn=conn, df=new_candles, table_name=table_name_us_selected_1min_candle_finnhub, method='copy_from_memory')


def load_1min_candle(conn, symbol):
    '''
    Retrive 1min candle from finnhub and load to staging and primary db
    '''
    last_min_candle_staging = get_last_min_candle(symbol, from_staging=True)
    if last_min_candle_staging is None or last_min_candle_staging.empty:
        load_full_min_candle_from_finnhub(conn, symbol)
    else:
        load_new_min_candle_from_finnhub(conn, symbol)
