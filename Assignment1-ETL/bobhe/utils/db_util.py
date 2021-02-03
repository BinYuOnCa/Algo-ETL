import finnhub
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
import psycopg2
from io import StringIO
from datetime import datetime
from utils.convert_date_time_format import convert_unix_date_format
from utils.convert_date_time_format import get_today_date
from utils.sms import send_twilio_message
from utils.mail import send_gmail_message
import utils.param as param
import time
import os
import requests
from utils.log import output_log

# Setup finnhub client
finnhub_client = finnhub.Client(api_key=param.FINNHUB_API_TOKEN)


def get_engine():
    """
    create database engine for postgres
    :param:
    :return engine: (obj)
    """
    engine = create_engine(f'postgresql://postgres:'
                           f'{param.DB_PASSWORD}@'
                           f'{param.DB_HOST}/'
                           f'{param.DB_NAME}')
    return engine


def get_stock_candles_via_liabary(symbol,
                                  interval,
                                  start_date,
                                  end_date):
    """
    get stock candles data from finnhub
    :param symbol: (str)
    :param interval: (str) ,  'D' for daily, '1' for one minutes
    :param start_date: (int) , unix time
    :param end_date: (int), unix time
    :return pandas dataframe
    """
    context = f'stock:{symbol} getting data from finnhub ' \
              f'through calling stock_candles() with param ' \
              f'interval={interval} between {start_date} and {end_date}'
    try:
        res = finnhub_client.stock_candles(symbol,
                                           interval,
                                           start_date,
                                           end_date)
    except finnhub.exceptions.FinnhubAPIException as api_exception:
        output_log(f'{context} {api_exception}', param.LOG_LEVEL_ERROR)
        send_twilio_message(f'{context} {api_exception}')
        send_gmail_message('ERROR - ETL getting data exception',
                           f'{context} {api_exception}')
        return pd.DataFrame({'A': []})
    except Exception as general_exception:
        output_log(f'{context} {general_exception}', param.LOG_LEVEL_ERROR)
        send_twilio_message(f'{context} {general_exception}')
        send_gmail_message('ERROR - ETL getting data exception',
                           f'{context} {general_exception}')
        return pd.DataFrame({'A': []})
    else:
        if res['s'] != 'no_data':
            return pd.DataFrame(res)
        else:
            output_log(f'{context} returned status: no_data',
                       param.LOG_LEVEL_WARNING)
            send_twilio_message(f'{context} returned status: no_data')
            send_gmail_message('WARNING - ETL process stock data missing',
                               f'{context} returned status: no_data')
            return pd.DataFrame({'A': []})


def get_stock_candles_via_request_url(symbol,
                                      interval,
                                      start_date,
                                      end_date):
    """
    get stock candles data from finnhub via url request
    :param symbol: (str)
    :param interval: (str) ,  'D' for daily, '1' for one minutes
    :param start_date: (int) , unix time
    :param end_date: (int), unix time
    :return pandas dataframe
    """
    context = f'stock:{symbol} getting data from finnhub ' \
              f'through https request with param ' \
              f'interval={interval} between {start_date} and {end_date}'
    request_url = f'{param.FINNHUN_REQUEST_URL}' \
                  f'symbol={symbol}&' \
                  f'resolution={interval}&' \
                  f'from={start_date}&' \
                  f'to={end_date}&' \
                  f'token={param.FINNHUB_API_TOKEN}'
    try:
        res = requests.get(request_url)
    except Exception as general_exception:
        output_log(f'{context} {general_exception}', param.LOG_LEVEL_ERROR)
        send_twilio_message(f'{context} {general_exception}')
        send_gmail_message('ERROR - ETL getting data exception',
                           f'{context} {general_exception}')
        return pd.DataFrame({'A': []})
    else:
        if res.json()['s'] != 'no_data':
            return pd.DataFrame(res.json())
        else:
            output_log(f'{context} returned status: no_data',
                       param.LOG_LEVEL_WARNING)
            send_twilio_message(f'{context} returned status: no_data')
            send_gmail_message('WARNING - ETL process stock data missing',
                               f'{context} returned status: no_data')
            return pd.DataFrame({'A': []})


def check_stock_status(symbol):
    """
    Check candles data availability or not from finnhub
    :param symbol: (str)
    :return status: (bool) STOCK_DATA_AVAILABLE
                        or STOCK_DATA_NOT_AVAILABLE
    """
    res = finnhub_client.quote(symbol)
    if res['c'] == 0:
        # {"c":0,"h":0,"l":0,"o":0,"pc":0,"t":0}
        output_log(f'symbol : {symbol} data is not available in finnhub',
                   param.LOG_LEVEL_WARNING)
        return param.STOCK_DATA_NOT_AVAILABLE
    else:
        # {"c":139.07,"h":139.85,"l":135.02,"o":136.28,
        # "pc":136.87,"t":1611360000}
        return param.STOCK_DATA_AVAILABLE


def get_close_price_of_last_trade_date2():
    """
    Getting the each symbol's close price of last trade date
    from database daily prices table
    :param:
    :return pandas dataframe
    """
    query = """WITH max_dp AS (SELECT MAX(utc_unix_time) AS t1, symbol AS s1
    FROM daily_prices group by symbol)
    SELECT s1 , t1 , close FROM max_dp m1, daily_prices dp
    WHERE m1.t1 = dp.utc_unix_time AND m1.s1 = dp.symbol"""

    engine = get_engine()

    try:
        result = engine.execute(query)
        df = DataFrame(result.fetchall(),
                       columns=['symbol',
                                'utc_unix_time',
                                'close'])
        df.set_index('symbol')
        return df

    except Exception as e:
        output_log(e, param.LOG_LEVEL_ERROR)
        return pd.DataFrame({'A': []})


def check_price_split_occur(symbol, df):
    """
    check if prices split happen for the specific symbol
    :param symbol:(str)
    :param df:(pandas dataframe)
    :return status: (bool) True or False
    """
    new_close_price = 0.0
    if df.loc[df['symbol'] == symbol].empty:
        output_log(f'Return false to skip split handing as '
                   f'no daily data in db for symbol: {symbol}')
        return False
    else:
        utc_unix_time = df.loc[df['symbol'] == symbol,
                               'utc_unix_time'].iloc[0]
        old_close_price = round(df.loc[df['symbol'] == symbol,
                                'close'].iloc[0], 2)

    output_log(f'old_close_price={old_close_price} '
               f'at time:{utc_unix_time} form daily_prices table')
    # get the symbol new close price from finhnub for this time
    time.sleep(1)
    df_new = get_stock_candles_via_liabary(symbol,
                                           param.INTERVAL_ONE_DAY,
                                           utc_unix_time,
                                           utc_unix_time)
    if not df_new.empty:
        new_close_price = round(df_new.loc[df_new.index[0], 'c'], 2)
        output_log(f'new_close_price={new_close_price} from finnhub')
    else:
        if check_stock_status(symbol) != param.STOCK_DATA_AVAILABLE:
            output_log(f'Stock data not available , skip split handing'
                       f' for symbol: {symbol}')
            return False
        else:
            output_log(f'stock {symbol} close_price does not exist '
                       f'for the requested unix time in finnub')
            output_log('split is required')
            return True
    # check the close price of last trading day if same
    # between new retrieved data and existing data in db
    if new_close_price == old_close_price:
        # output_log('split handling does not require')
        return False
    else:
        output_log('change on close price, split handling is required')
        return True


def import_one_off_bar_file(path_file_name, table_name):
    """
    import one off bar csv files to db
    by using psycopg2 copy_from function
    :param path_file_name:(str)
    :param table_name:(str)
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_NORMAL
    output_log(f'start import_one_off_bar_file : {path_file_name}')
    data = pd.read_csv(path_file_name, index_col=[0])

    try:
        # convert dataframe to StringIO
        output = StringIO()
        data.to_csv(output, sep='\t', index=False, header=False)
        output1 = output.getvalue()
        conn = psycopg2.connect(host=param.DB_HOST,
                                user=param.DB_USER,
                                password=param.DB_PASSWORD,
                                database=param.DB_NAME)
        cur = conn.cursor()
        cur.copy_from(StringIO(output1), table_name)
    except Exception as e:
        output_log(e, param.LOG_LEVEL_ERROR)
        output_log(f'import_one_off_bar_file:{path_file_name} failed')
        status = param.PROCESS_FAILURE
    else:
        conn.commit()
        output_log(f'import_one_off_bar_file:{path_file_name} successfully')
    finally:
        cur.close()
        conn.close()
        return status


def get_symbol_from_file_name(file_name):
    """
    get the symbol name from the file name
    :param file_name:(str) AAPL_2021-01-13.csv
    :return (str) symbol value in the file name
    """
    # AAPL_2021-01-13.csv
    return file_name.split('_')[0]


def get_symbol_from_path_file_name(path_file_name):
    """
    get the symbol name from the path and file name
    :param path_file_name :(str) ./data/daily/AAPL_2021-01-23.csv
    :return (str) symbol name
    """
    # ./data/daily/AAPL_2021-01-23.csv
    # ./data/one_off_min/ZION_min_batch13_2021-01-23.csv
    str1 = path_file_name.split('/')[3]
    str2 = str1.split('_')[0]
    return str2


def delete_existing_daily_and_min_bar_data(symbol):
    """
    Delete all the existing daily and min bar data
    :param symbol :(str)
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    # initialize status
    status = param.PROCESS_NORMAL
    engine = get_engine()
    query = """DELETE FROM daily_prices
            WHERE symbol = '{}';""".format(symbol)
    try:
        engine.execute(query)
    except Exception as e:
        output_log(e, param.LOG_LEVEL_ERROR)
        status = param.PROCESS_FAILURE
        return status
    query = """DELETE FROM min_prices
            where symbol = '{}';""".format(symbol)
    try:
        engine.execute(query)
    except Exception as e:
        output_log(e, param.LOG_LEVEL_ERROR)
        status = param.PROCESS_FAILURE
    return status


# handle split case
def handle_stock_split(symbol):
    """
    handle stock split scenario
    :param symbol :(str)
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_NORMAL
    output_log(f'handle stock split scenario for symbol: {symbol}')
    # delete the existing daily bar data in db
    if delete_existing_daily_and_min_bar_data(symbol) \
            != param.PROCESS_NORMAL:
        status = param.PROCESS_FAILURE
        return status
    output_log('delete existing data successfully')
    one_off_start_date = convert_unix_date_format(
                        param.DAILY_BAR_DATA_ONE_OFF_START_DATE)
    one_off_end_date = convert_unix_date_format(get_today_date())
    # get and save the historical daily bars data as csv file
    if get_and_save_daily_bars_data(symbol,
                                    one_off_start_date,
                                    one_off_end_date) \
            != param.PROCESS_NORMAL:
        status = param.PROCESS_FAILURE
        return status
    output_log(f'get and save daily bars data successfully for: {symbol}')
    # Build up the file name and file path
    path_file_name = symbol_to_path(symbol,
                                    param.ONE_OFF_DAILY_BARS_PATH)
    # Reload the historical daily bar data into db
    if import_one_off_bar_file(path_file_name,
                               param.TABLE_NAME_DAILY_PRICES) \
            != param.PROCESS_NORMAL:
        status = param.PROCESS_FAILURE
        output_log('import_one_off_bar_file failed in split handing')
        return status
    # get and save the historical min bars data as csv file
    min_bar_data_one_off_end_date = convert_unix_date_format(
        get_today_date())
    min_bar_data_one_off_start_date = min_bar_data_one_off_end_date \
        - param.UNIX_ONE_DATE * 365
    if get_and_save_intraday_bars_data(symbol,
                                       param.INTERVAL_ONE_MINUTE,
                                       min_bar_data_one_off_start_date,
                                       min_bar_data_one_off_end_date)\
            != param.PROCESS_NORMAL:
        output_log('get_and_save_intraday_bars_data failed',
                   param.LOG_LEVEL_WARNING)
        return param.PROCESS_FAILURE
    output_log(f'get and save historical min bars_data '
               f'successfully for: {symbol}')
    # Reload the historical intraday minutes bar data into db
    if upload_one_off_bars_file(param.ONE_OFF_MIN_BARS_PATH,
                                param.TABLE_NAME_MINS_PRICES,
                                symbol) \
            != param.PROCESS_NORMAL:
        output_log(f'handle split,upload_one_off_bars_file '
                   f'for {symbol} failed ', param.LOG_LEVEL_WARNING)
        status = param.PROCESS_FAILURE
    else:
        status = param.PROCESS_NORMAL
        output_log(f'handle split,upload_one_off_bars_file '
                   f'for {symbol} successful ')
    return status


def process_insert_table_daily_prices(df):
    """
    insert data into the daily_prices table
    :param df :(pandas dataframe)
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    # initialize status
    status = param.PROCESS_NORMAL
    # First part of the insert statement
    insert_init = """INSERT INTO daily_prices
                    (symbol,
                    close,
                    high,
                    low,
                    open,
                    status,
                    utc_unix_time,
                    utc_readable_time,
                    volume)
                    VALUES
                """
    # Add values for all days to the insert statement
    vals = ",".join(["""('{}', '{}', '{}', '{}', '{}',
                     '{}', '{}', '{}', '{}')""".format(
        row.symbol,
        row.close,
        row.high,
        row.low,
        row.open,
        row.status,
        row.utc_unix_time,
        row.utc_readable_time,
        row.volume
    ) for index, row in df.iterrows()])
    # Handle duplicate values
    # Avoiding errors if you've already got some data in your table
    insert_end = """ ON CONFLICT (symbol, utc_unix_time) DO NOTHING"""
    # Put together the query string
    query = insert_init + vals + insert_end
    engine = get_engine()
    # Fire insert statement
    try:
        engine.execute(query)
    except Exception as e:
        status = param.PROCESS_FAILURE
        output_log(e, param.LOG_LEVEL_ERROR)
    return status


# process dataframe minute data and insert to min_prices table
def process_insert_table_min_prices(df):
    """
    insert data into the min_prices table
    :param df :(pandas dataframe)
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    # initialize status
    status = param.PROCESS_NORMAL
    # First part of the insert statement
    insert_init = """INSERT INTO min_prices
                    (symbol,
                    close,
                    high,
                    low,
                    open,
                    status,
                    utc_unix_time,
                    local_time,
                    volume)
                    VALUES
                """
    # Add values for all days to the insert statement
    vals = ",".join(["""('{}', '{}', '{}', '{}', '{}',
                     '{}', '{}', '{}', '{}')""".format(
        row.symbol,
        row.close,
        row.high,
        row.low,
        row.open,
        row.status,
        row.utc_unix_time,
        row.local_time,
        row.volume
    ) for index, row in df.iterrows()])
    # Handle duplicate values - Avoiding errors
    # if you've already got some data in your table
    insert_end = """ ON CONFLICT (symbol, utc_unix_time) DO NOTHING"""
    # Put together the query string
    query = insert_init + vals + insert_end
    engine = get_engine()
    # Fire insert statement
    try:
        engine.execute(query)
    except Exception as e:
        output_log(e, param.LOG_LEVEL_ERROR)
        status = param.PROCESS_FAILURE
    return status


def import_incremental_bars_file(path_file_name,
                                 table_name,
                                 df_last_trade_date):
    """
    inport icremental bars file into database
    :param path_file_name: (str)
    :param table_name: (str)
    :param df_last_trade_date: (pandas dataframe)
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    output_log(f'start import_incremental_bars_file: {path_file_name} '
               f'for table: {table_name}')
    status = param.PROCESS_NORMAL
    symbol = get_symbol_from_path_file_name(path_file_name)
    # check if price split occur
    if check_price_split_occur(symbol, df_last_trade_date):
        status = handle_stock_split(symbol)
        if status == param.PROCESS_NORMAL:
            output_log(f'handle split successfully for: {path_file_name}')
        else:
            output_log(f'handle split failure for: {path_file_name}')
    else:
        # insert daily incremental bar data into db
        df = pd.read_csv(path_file_name, index_col=[0])
        # Rename some column names
        df = df.rename(columns={"c": "close", "h": "high",
                                "l": "low", "o": "open", "s": "status",
                                "t": "utc_unix_time", "v": "volume"})

        if table_name == param.TABLE_NAME_DAILY_PRICES:
            status = process_insert_table_daily_prices(df)
        else:
            status = process_insert_table_min_prices(df)
        output_log(f'Insert incremental bar file into table:{table_name} '
                   f'successfully for: {path_file_name}')
    return status


def get_first_trade_date():
    """
    get the first trade date from the daily_prices table
    :param:
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    output_log('get_first_trade_date')
    status = param.PROCESS_NORMAL
    query = """SELECT MIN(utc_readable_time) FROM daily_prices;"""
    engine = get_engine()
    try:
        result = engine.execute(query)
        row = result.fetchone()
        if row[0]:
            param.min_trade_date = row[0]
            param.min_trade_date = param.min_trade_date[0:10]
            output_log(f'min_trade_date: {param.min_trade_date}')
        else:
            output_log('Table daily_prices is empty', param.LOG_LEVEL_WARNING)
        return status
    except Exception as e:
        output_log(e, param.LOG_LEVEL_ERROR)
        status = param.PROCESS_FAILURE
        return status


def get_last_trade_date():
    """
    get the last trade date from the daily_prices table
    :param:
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    output_log('get_last_trade_date')
    status = param.PROCESS_NORMAL
    query = """SELECT MAX(utc_readable_time) FROM daily_prices
    WHERE volume <> 0;"""
    engine = get_engine()
    try:
        result = engine.execute(query)
        row = result.fetchone()
        if row[0]:
            param.max_trade_date = row[0]
            # get first 10 char , like: 2021-01-07
            param.max_trade_date = param.max_trade_date[0:10]
            output_log(f'max_trade_date: {param.max_trade_date}')
            return status
        else:
            output_log('Table daily_prices is empty', param.LOG_LEVEL_WARNING)
            status = param.PROCESS_FAILURE
            return status
    except Exception as e:
        output_log(e, param.LOG_LEVEL_ERROR)
        status = param.PROCESS_FAILURE
        return status


def get_sp500_stock_synbols_list():
    """
    get the S&P 500 stock symbols from wiki
    :param:
    :return (pandas dataframe)
    """
    # get stock symbol
    table = pd.read_html(param.SP500_SYMBOL_URL)
    df = table[0]
    df = df.sort_values(by='Symbol', ascending=True)
    return df


def symbol_to_path(symbol, base_dir):
    """
    set up the file path and file
    :param symbol: (str)
    :param base_dir: (str)
    :return (str)
    """
    return os.path.join(base_dir,
                        "{}.csv".format(str(symbol) +
                                        '_' +
                                        get_today_date()))


def save_data(df, filename, index_required):
    """
    save data into csv file
    :param df: (pandas dataframe)
    :param filename: (str)
    :param index_required: (bool)
    :return:
    """
    # Save data to a csv file
    if index_required:
        df.to_csv(filename)
    else:
        df.to_csv(filename, index=False)


# Get daily bar data and save them as csv files into folder
def get_and_save_daily_bars_data(symbol, start_date, end_date):
    """
    get and save the daily bars data
    :param symbol: (str)
    :param start_date: (int) unix time format
    :param end_date: (int) unix time format
    :return status: (bool)  PROCESS_NORMAL or PROCESS_FAILURE
    """
    # Call finnhub api to retrive the daily bars data
    output_log(f'start get_and_save_daily_bars_data for symbol: {symbol}')
    data = get_stock_candles_via_liabary(symbol,
                                         param.INTERVAL_ONE_DAY,
                                         start_date,
                                         end_date)
    if data.empty:
        return param.PROCESS_NORMAL
    # Insert a new column : utc_readable_time
    data.insert(loc=6, column='utc_readable_time', value='')
    # Convert the unix time to human readable time format
    data.utc_readable_time = data.t.apply(
        lambda dt: datetime.utcfromtimestamp(dt).strftime('%Y-%m-%d %H:%M:%S'))
    # Convert to local time
    # data.est_time = data.t.apply(lambda dt: datetime.fromtimestamp(dt))
    # Insert symbol column at first column
    data.insert(loc=0, column='symbol', value=symbol)
    if (end_date - start_date) > param.UNIX_ONE_DATE * 365:
        # during > 1 year, build up the file name and file path for one-off
        file_path_name = symbol_to_path(symbol,
                                        param.ONE_OFF_DAILY_BARS_PATH)
    else:
        # Build up the file name and file path for daily
        file_path_name = symbol_to_path(symbol, param.DAILY_BARS_PATH)
    # Save the data into a csv
    save_data(data, file_path_name, index_required=True)
    return param.PROCESS_NORMAL


def download_daily_bars(start_date, end_date):
    """
    download the daily bars data for the whole S&P500 stock list
    :param start_date: (int) unix time format
    :param end_date: (int) unix time format
    :return status: (bool)  PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_NORMAL
    # get stock symbol
    df = get_sp500_stock_synbols_list()
    if df.empty:
        status = param.PROCESS_FAILURE
        return status
    for index, row in df.iterrows():
        symbol = row['Symbol']
        status = get_and_save_daily_bars_data(symbol,
                                              start_date,
                                              end_date)
        output_log(f'get_and_save_daily_bars_data status = {status} for '
                   f'symbol = {symbol}')
        # Sleep 1 second
        time.sleep(1)
    return status


def download_intraday_bars(interval, start_date, end_date):
    """
    download the intraday bars data for the whole S&P500 stock list
    :param interval: (str) '1' for one minutes
    :param start_date: (int) unix time format
    :param end_date: (int) unix time format
    :return status: (bool)  PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_NORMAL
    # get stock symbol
    df = get_sp500_stock_synbols_list()
    if df.empty:
        status = param.PROCESS_FAILURE
        return status
    for index, row in df.iterrows():
        symbol = row['Symbol']
        status = get_and_save_intraday_bars_data(symbol,
                                                 interval,
                                                 start_date,
                                                 end_date)
        # Sleep 1 second
        time.sleep(1)
    return status


# This function will loop through all the files in the directory
# and process the CSV files and upload the db
def upload_one_off_bars_file(file_path, table_name, symbol=None):
    """
    upload the one off bars file into database
    :param file_path: (str)
    :param table_name: (str)
    :param symbol: (str) optional
    :return status: (bool)  PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_FAILURE
    for symbol_bar_file_name in os.listdir(file_path):
        output_log(symbol_bar_file_name)
        if symbol:
            # get symbol name from the symbol_bar_file_name
            symbol_in_bar_file_name = get_symbol_from_file_name(
                symbol_bar_file_name)
            output_log(f'symbol_in_bar_file_name: {symbol_in_bar_file_name}')
            if symbol != symbol_in_bar_file_name:
                output_log(f'symbol_in_bar_file_name:{symbol_in_bar_file_name}'
                           f' not equal the requested symbol:{symbol}, '
                           f'skip import this bar file')
                continue
        path_file_name = file_path + symbol_bar_file_name
        status = import_one_off_bar_file(path_file_name, table_name)
        if status == param.PROCESS_FAILURE:
            continue
    return status


def upload_incremental_bars_file(file_path, table_name):
    """
    upload the incremental bars file into into database
    :param file_path: (str)
    :param table_name: (str)
    :return status: (bool)  PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_NORMAL
    df_last_close_price = get_close_price_of_last_trade_date2()
    if df_last_close_price.empty:
        status = param.PROCESS_FAILURE
        return status
    for symbol_bar_file_name in os.listdir(file_path):
        output_log(symbol_bar_file_name)
        path_file_name = file_path + symbol_bar_file_name
        status = import_incremental_bars_file(path_file_name,
                                              table_name,
                                              df_last_close_price)
        if status == param.PROCESS_FAILURE:
            continue
    return status


# This function will get the first min bar trade date
def get_first_min_bar_trade_date():
    """
    get the first trade date from min_prices table
    :param:
    :return status: (bool)  PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_NORMAL
    # Due to large volume data for min bar data
    # To increase the sql query performance,
    # specify APPLE stock symbol: 'AAPL' in the query
    query = """SELECT MIN(utc_unix_time) FROM min_prices
               WHERE symbol = 'AAPL' AND volume <> 0;"""
    engine = get_engine()
    try:
        result = engine.execute(query)
        row = result.fetchone()
        if row[0]:
            param.first_min_bar_trade_date = row[0]
            output_log(f'first_min_bar_trade_date: '
                       f'{param.first_min_bar_trade_date}')
        else:
            param.first_min_bar_trade_date = 0
            output_log('Table min_prices is empty', param.LOG_LEVEL_WARNING)
        return status
    except Exception as e:
        output_log(e, param.LOG_LEVEL_ERROR)
        param.first_min_bar_trade_date = 0
        status = param.PROCESS_FAILURE
        return status


def get_last_min_bar_trade_date():
    """
    get the last trade date from min_prices table
    :param:
    :return status: (bool)  PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_NORMAL
    query = """SELECT MAX(utc_unix_time) FROM min_prices
            WHERE symbol = 'AAPL' AND volume <> 0;"""
    engine = get_engine()
    try:
        result = engine.execute(query)
        row = result.fetchone()
        if row[0]:
            param.last_min_bar_trade_date = row[0]
            output_log(f'last_min_bar_trade_date: '
                       f'{param.last_min_bar_trade_date}')
        else:
            param.last_min_bar_trade_date = 0
            output_log('Table min_prices is empty', param.LOG_LEVEL_WARNING)
        return status
    except Exception as e:
        output_log(e, param.LOG_LEVEL_ERROR)
        param.last_min_bar_trade_date = 0
        status = param.PROCESS_FAILURE
        return status


def get_and_save_intraday_bars_data(symbol, interval, start_date, end_date):
    """
    get the save the intraday bars data into a csv file
    :param symbol: (str)
    :param interval: (str) '1' for one minutes, 'D' for one day
    :param start_date: (int) unix time format
    :param end_date: (int) unix time format
    :return stats: (bool)  PROCESS_NORMAL or PROCESS_FAILURE
    """
    output_log(f'start get_and_save_intraday_bars_data for symbol: {symbol}')
    status = param.PROCESS_NORMAL
    if (end_date - start_date) > param.UNIX_ONE_DATE * 31:
        # handle one-off load if the duration is greater than 1 month
        output_log(f'handle one-ff min data for symbol: {symbol}')
        new_start_date = start_date
        for i in range(13):
            new_end_date = param.UNIX_ONE_DATE * 31 + new_start_date
            data = get_stock_candles_via_request_url(symbol,
                                                     interval,
                                                     new_start_date,
                                                     new_end_date)
            if data.empty:
                continue
            # output_log(data.head())
            # Insert a new column : local_time
            data.insert(loc=6, column='local_time', value='')
            # Convert to local time
            data.local_time = data.t.apply(
                lambda dt: datetime.fromtimestamp(dt))
            # Insert symbol column at first column
            data.insert(loc=0, column='symbol', value=symbol)
            # Build up the file name and file path
            file_name = symbol + '_min_' + 'batch' + str(i + 1)
            file_path_name = symbol_to_path(file_name,
                                            param.ONE_OFF_MIN_BARS_PATH)
            # Save the data into a csv
            save_data(data, file_path_name, index_required=True)
            new_start_date = new_end_date - param.UNIX_ONE_DATE * 1
            time.sleep(1)
    else:
        # handle everyday min dara
        data = get_stock_candles_via_request_url(symbol,
                                                 interval,
                                                 start_date,
                                                 end_date)
        if data.empty:
            return status
        # Insert a new column : local_time
        data.insert(loc=6, column='local_time', value='')
        # Convert to local time
        data.local_time = data.t.apply(lambda dt: datetime.fromtimestamp(dt))
        # Insert symbol column at frist column
        data.insert(loc=0, column='symbol', value=symbol)
        # Build up the file name and file path
        file_name = symbol + '_min'
        file_path_name = symbol_to_path(file_name, param.MIN_BARS_PATH)
        # Save the data into a csv
        save_data(data, file_path_name, index_required=True)
        time.sleep(1)
    return status
