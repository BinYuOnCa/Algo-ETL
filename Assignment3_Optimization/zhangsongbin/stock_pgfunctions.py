import psycopg2
import sys
import calendar
import time
from sqlalchemy import create_engine
import pandas as pd
from utils.stock_settings import Settings
from utils import stock_csv_functions as csv
from utils import stock_other_functions as oth
from utils import stock_time as myt
from io import StringIO

output = StringIO()

"""
There are the functions about database.
Connect to database.
Execute a SQL.
Query column in one table.
Query the max timestamp in one table.
Clear one table's data.
Import CSV to database by copy_from.
Import CSV to database by to_sql.
Import tuples to database by SQL.
"""


stock_settings = Settings()
param_dic = stock_settings.param_dic


def connect():
    """
    Connect to the PostgreSQL database server.
    :return: conn or None
    """
    try:
        conn = psycopg2.connect(**param_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"error:{error},param_dic:{param_dic}")
        oth.log(error)
        oth.send_sms(f"conn has an error:{error}")
        sys.exit(1)
        return None
    return conn


def execute_sql(sql):
    """
    Query any sql in one table
    :param sql: str
    :return: results:data or None
    """
    conn_db = connect()
    cursor = conn_db.cursor()
    try:
        cursor.execute(sql)
    except Exception as error:
        print(f"error:{error}")
        oth.log(error)
        cursor.close()
        conn_db.close()
        return None
    results = cursor.fetchall()
    conn_db.commit()
    cursor.close()
    conn_db.close()
    return results




def database_to_pd(sql):
    """
    输入SQL查询语句，用pandaframe来得到pd数据集格式的查询结果
    :param sql: str 字符串
    :return: pd格式的数据集
    """
    conn = connect()
    results = pd.read_sql(sql, conn)
    conn.close()
    return results


def get_symbol_df(start_date: int, end_date: int):
    """
    输入起始和终止日期，拿到这期间内得的全部的股票的清单，返回DataFrame数据集。
    因为并非所有股票在这个日期期间内都有，有的股票或许已经退市了，有的或许这期间还没上市。
    :param start_date: int 格式是20200601
    :param end_date: int  格式是20201228
    :return: DataFrame
    """
    st = myt.dateint_stamp(start_date, "begin")
    et = myt.dateint_stamp(end_date, "end")
    # sql = f"select distinct symbol from stock_candles_day where t>{st} and t<{et} and c>0"
    sql = f"select distinct symbol from stock_candles_day where t>{st} and t<{et} and c>0 and symbol='TSLA' "
    # print(sql)
    conn = connect()
    stock_list_result = pd.read_sql(sql, conn)
    conn.close()
    return stock_list_result