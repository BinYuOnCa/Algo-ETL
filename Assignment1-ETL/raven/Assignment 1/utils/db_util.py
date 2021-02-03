import psycopg2
try:
    from config import DB_CONFIG
except ModuleNotFoundError:
    from .config import DB_CONFIG

def connect_to_db():
    conn = psycopg2.connect(
        database=DB_CONFIG["DATABASE"],
        user=DB_CONFIG["USER"],
        password=DB_CONFIG["PASSWORD"],
        host=DB_CONFIG["HOST"]
    )
    return conn

def get_stock_list(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT symbol FROM stock_list_test")
        records = cur.fetchall()
        print(records)
        cur.close()
        return records
    except:
        print("No stock in the watch list. Please try again.")

def load_into_1m_db(conn, stock, open_price, close_price, high_price, low_price, volumn, datetime):
    cur = conn.cursor()
    sql_query = \
        "INSERT INTO us_equity_1m_finn (symbol, open_price, close_price, high_price, low_price, volumn, datetime) values('"\
        + str(stock) + "'," + str(open_price) + "," + str(close_price) + "," + str(high_price) + "," + str(low_price) + "," + str(volumn)\
        + ",'" + str(datetime) + "')"
    # print(sql_query)
    cur.execute(sql_query)
    conn.commit()
    # print("Value inserted")

def load_into_daily_db(conn, stock, open_price, close_price, high_price, low_price, volumn, datetime):
    cur = conn.cursor()
    sql_query = \
        "INSERT INTO us_equity_daily_finn (symbol, open_price, close_price, high_price, low_price, volumn, datetime) values('"\
        + str(stock) + "'," + str(open_price) + "," + str(close_price) + "," + str(high_price) + "," + str(low_price) + "," + str(volumn)\
        + ",'" + str(datetime) + "')"
    # print(sql_query)
    cur.execute(sql_query)
    conn.commit()
    # print("Value inserted")


def disconnect_db(conn):
    conn.close()
