import psycopg2
import sys
import time
from sqlalchemy import create_engine
import pandas as pd
from utils.stock_settings import Settings
from utils import stock_csv_functions as csv
from utils import stock_other_functions as oth


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


def column(table, col):
    """
    Query column in one table
    :param table: str
    :param col: str
    :return: pg_column_dt:data or None
    """
    try:
        conn_db = connect()
    except IOError as error:
        print(f"There is an IO error when connecting to the database.{error}")
        oth.log(error)
        return None
    cursor = conn_db.cursor()
    sql = f"select distinct {col} from  {table} order by {col}  "
    # sql = "select distinct symbol from  stock_symbol_thousand where symbol='IBM' order by symbol  "
    try:
        cursor.execute(sql)
    except (Exception, psycopg2.errors.SyntaxError) as error:
        oth.log(error)
        cursor.close()
        conn_db.close()
        return None
    results = cursor.fetchall()
    if results == []:
        print(f"The {table} is empty")
        pg_column_dt = None
    else:
        print(type(results))
        pg_column_dt = results  # this is a list
    conn_db.commit()
    cursor.close()
    conn_db.close()
    return pg_column_dt


def max_t(table, symbol_name):
    """
    Query the max timestamp in one table
    :param table: str
    :param symbol_name: str
    :return: pg_max_dt:int
    """
    try:
        conn_db = connect()
    except IOError as error:
        print("There is an IO error when connecting to the database.")
        oth.log(error)
        return None
    cursor = conn_db.cursor()
    # sql = "select * from "+table+" where symbol='"+symbol_name+"' order by series desc limit 1"
    sql = f"select * from {table} where symbol='{symbol_name}' order by series desc limit 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    # print (results)
    if results == []:
        # print ("empety table")
        pg_max_dt = 0
    else:
        pg_max_dt = results[0][6]
    conn_db.commit()
    cursor.close()
    conn_db.close()
    return pg_max_dt


#
def clear_table(table):
    """
    Clear one table's data
    :param table: str
    :return: 1 or None
    """
    try:
        conn_db = connect()
    except IOError as error:
        print("There is an IO error when connecting to the database.")
        oth.log(error)
        return None
    cur = conn_db.cursor()
    sql = f"delete from {table} where 1=1"
    try:
        cur.execute(sql)
    except(Exception, psycopg2.DatabaseError) as error:
        oth.log(error)
        cur.close()
        conn_db.close()
        return None
    conn_db.commit()
    cur.close()
    conn_db.close()
    print("Clear table done.")
    return 1


def pg_to_sql(table, path, columns, symbol=""):
    """
    Import dataset to PostregSQL.very fast than old version.
    :param table: str
    :param path: str
    :param columns: list
    :param symbol: str
    :return: 1 or None
    """
    print("Begin import data.")
    begin_time = time.time()
    f = open(path, 'r')
    conn = connect()
    cursor = conn.cursor()
    # 使用copy_from 数据必须是无标题
    try:
        cursor.copy_from(f, table, sep=",", columns=columns)
        conn.commit()
    except psycopg2.errors.InvalidTextRepresentation as error:
        print(f"Invalid input syntax for type:{error}. Symbol:{symbol}. But pass to continue.")
        oth.log(error, symbol)
        conn.rollback()
        cursor.close()
        csv.clear_csv(path)
        return None
    except psycopg2.DatabaseError as error:
        # clear_csv(path)
        print(f"Import database error:{error}.Symbol:{symbol}")
        oth.log(error, symbol)
        conn.rollback()
        cursor.close()
        csv.clear_csv(path)
        return None
    except Exception as error:
        print(f"Import error:{error}.Symbol:{symbol}")
        oth.log(error, symbol)
        conn.rollback()
        cursor.close()
        csv.clear_csv(path)
        return None
    cursor.close()
    end_time = time.time()
    print(f"Import to database cost :{round(end_time-begin_time, 2)}")
    csv.clear_csv(path)
    return 1


def pg_to_sql_company_profile(table, path, symbol=""):
    """
    Only used by import company profile data to database.
    :param table: str
    :param path: str
    :param symbol: str
    :return: 1 or None
    """
    csv.add_csv_title(path, stock_settings.profile_title)
    print(path)
    dataset = pd.read_csv(path)
    begin_time = time.time()
    pq_info = "postgresql+psycopg2://"+param_dic["user"]+":"+param_dic["password"]+"@"+param_dic["host"]+"/"+param_dic["database"]
    engine = create_engine(pq_info)
    try:
        dataset.to_sql(table, engine, index=False, chunksize=None, if_exists='append', method="multi")
    except (Exception) as error:
        print(f"Company profile to_sql error:{error}")
        oth.log(error, symbol)
        return None
    csv.clear_csv(path)
    end_time = time.time()
    print("Import to database cost :%.2f " % round(end_time-begin_time, 2))
    return 1


def insert7(table, df):
    """
    插入数据，数据表为7列
    :param table: str
    :param df: data
    :return: 1 or None
    """
    # Using cursor.executemany() to insert the dataframe
    # Create a list of tuples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # 再插入数据
    conn = connect()
    query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cur = conn.cursor()
    try:
        cur.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        oth.log(error)
        conn.rollback()
        cur.close()
        return None
    print("execute_many() done")
    cur.close()
    return 1
