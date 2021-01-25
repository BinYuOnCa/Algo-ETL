import psycopg2
import os



# Connect to an existing database
def db_connect():
    conn = psycopg2.connect(database=os.environ['DB_DB'],
                            user=os.environ['DB_USERNAME'],
                            password=os.environ['DB_PASSWORD'],
                            host=os.environ['DB_HOST'],
                            port=os.environ['DB_PORT']
                        )
    return conn

conn = db_connect()

# Open a cursor to perform database operations
cur = conn.cursor()

# Create Table
cur.execute("""
    CREATE TABLE stock_candles_daily(
    close numeric,
    high numeric,
    low numeric,
    open numeric,
    status varchar(10),
    volumn bigint,
    datetime timestamp with time zone,
    symbol varchar(10)
)
""")

cur.execute("""
    CREATE TABLE stock_candles_minute(
    close numeric,
    high numeric,
    low numeric,
    open numeric,
    status varchar(10),
    volumn bigint,
    datetime timestamp with time zone,
    symbol varchar(10)
)
""")
conn.commit()


