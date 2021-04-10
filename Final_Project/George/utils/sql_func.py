import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse

import utils.db_func as db_func
import config.config_parser as conf
import utils.convert_time_timestamp as ctt


"""functions related to sql instructions"""

db_conn = db_func.DB_Conn()
conn = db_conn.create_connection()
sqlalchemy_engine = db_conn.create_sqlalchemy_engine()


def get_ticker_to_update_df(_conn=conn, col_name="ticker",
                            table_name="update_ticker_list",
                            exchange=str(conf.settings()["exchange"]).lower()):
    return pd.read_sql(f"select {col_name} from {table_name} "
                       f"where create_date = (select max(create_date) from {table_name})", _conn)

def get_last_timestamp_df(table_name, _conn=conn):
    return pd.read_sql(f"select symbol, max(finn_timestamp) as finn_timestamp from {table_name} "
                           f"group by symbol" , _conn)

def insert_df_to_db(df, table_name,sqlalchemy_engine=sqlalchemy_engine,
                    sqlserver_engine=conf.settings()["db_engine"]):
    if sqlserver_engine == "mssql":
        df.to_sql(table_name, sqlalchemy_engine, if_exists="append", index=False)
    elif sqlserver_engine == "postgresql":
        df.to_sql(table_name, sqlalchemy_engine, if_exists="append", index=False, method="multi")

def copy_csv_to_db(file, table_name, _conn=conn, sqlserver_engine=conf.settings()["db_engine"]):
    if sqlserver_engine == "postgresql":
        cursor = _conn.cursor()
        with open(file, 'r') as f:
            # Notice that we don't need the `csv` module.
            next(f)  # Skip the header row.
            cursor.copy_from(f, table_name, sep=',')
        _conn.commit()

def insert_df_to_db_iter(df, table_name="split", _conn=conn):
    try:
        cursor = _conn.cursor()
        for index, row in df.iterrows():
            cursor.execute(
                f"insert into {table_name} "
                f"(ticker, split_date, from_factor, to_factor) values "
                f"('{row.symbol}', '{parse(row.date).date()}', {row.fromFactor}, {row.toFactor})")
        _conn.commit()
    except Exception as e:
        with open(Path(__file__).parent / "../logs/finn_log.log", "a") as f:
            f.write('"' + str(datetime.today()) +
                    '", "Something is wrong when executing the get_split_df", "' + str(e) + '"\n')

def get_symbol_close_price(symbol, table_name, date=None, _conn=conn):
    if date is None:

        date = get_symbol_max_date(symbol, table_name)
    close_price = pd.read_sql(f"select max(close_price) as close_price from {table_name} "
                       f"where symbol = '{symbol}' and date_int_key = '{date}'", _conn)
    return close_price if len(close_price) != 0 else None

def get_symbol_max_date(symbol, table_name, _conn=conn):
    max_date = pd.read_sql(f"select max(date_int_key) as max_date from {table_name} where symbol = '{symbol}'", _conn)
    return max_date.iloc[0, 0]


def get_valid_dates(conn,
                    date_from=(datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
                    date_until=datetime.now().strftime("%Y-%m-%d")):
    start_date = datetime.strptime(str(date_from), '%Y%m%d').strftime('%Y-%m-%d')
    end_date = datetime.strptime(str(date_until), '%Y%m%d').strftime('%Y-%m-%d')
    valid_date_df = pd.read_sql(f"SELECT * FROM valid_dates "
                                f"where date_int_key between '{date_from}' AND '{date_until}'", conn)
    return valid_date_df['date_int_key'].apply(lambda x: x.strftime('%Y%m%d')).astype(int).tolist()


def download_data_daily(conn, security_symbol, start_date, end_date, interval='daily'):
    start_date = datetime.strptime(str(start_date), '%Y%m%d').strftime('%Y-%m-%d')
    end_date = datetime.strptime(str(end_date), '%Y%m%d').strftime('%Y-%m-%d')
    daily_data = pd.read_sql(f"SELECT * FROM us_equity_finn_daily "
                       f"where symbol='{security_symbol}' AND (date_int_key between '{start_date}' AND '{end_date}') order by date_int_key ASC", conn)
    daily_data.drop(columns=['symbol', 'id','timestamp', 'finn_timestamp'], inplace=True)
    daily_data['date_int_key'] = daily_data['date_int_key'].apply(lambda x: x.strftime('%Y%m%d')).astype(int)
    return daily_data


def get_latest_datadate(conn=conn):
    max_date = pd.read_sql("select max(date_int_key) from us_equity_finn_daily", conn)
    return max_date.iloc[0][0]


def calculate_high():
    pass

def insert_purchase(stock_purchase, table_name="stock_purchase", conn=conn):
    with conn.cursor() as cursor:
        for index, row in stock_purchase.iterrows():
            cursor.execute(f"INSERT INTO stock_purchase (ticker, mean, std, ranks, buying_date, sale_interval, sold) VALUES ('{row.ticker}', {row['mean']}, {row['std']}, {row['ranks']}, '{row.buying_date}', {row.sale_interval}, {row.sold})")
        conn.commit()


def get_all_slice_size(conn, table_name="stock_purchase"):
    sold_interval = pd.read_sql(f"select sale_interval, count(*) as count_number from {table_name} where sold=0 group by sale_interval order by sale_interval ASC", conn)
    return sold_interval

def get_stock_buy_df_by_buy_date(conn, buy_date, table_name="stock_purchase"):
    stock_list = pd.read_sql(f"select * from {table_name} where buying_date = '{buy_date}'",conn)
    return stock_list

if __name__ == "__main__":
    #print(download_data_daily(conn, 'TSLA', 20201208, 20210108))
    print(get_latest_datadate())
    pass