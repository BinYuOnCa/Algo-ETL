import pandas as pd
from pathlib import Path
from datetime import datetime
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
