import pandas as pd
from io import StringIO
import psycopg2
import finnhub
import db_utility as util
import logging
import time
import datetime





#  Verify the target db table is existing
def check_Table(table_name):
    try:
        conn = util.cursor_setup()
        cur = conn.cursor()
        # create table one by one
        sqlcommand = "SELECT 1 FROM information_schema.tables WHERE " \
                     "table_schema='public' AND table_name='"+table_name+"'"
        cur.execute(sqlcommand)
        if cur.fetchone() is not None:
            return True
        else:
            return False

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')

# retrieve the latest timestamp for a symbol
def retrieve_latestSymbol(symbol, table_name):
    try:
        conn = util.cursor_setup()
        cur = conn.cursor()
        sqlcommand = "CREATE INDEX IF NOT EXISTS idx_time ON "+table_name+" (symbol);"
        cur.execute(sqlcommand)
        conn.commit()
        # Is this SELECT query effective enough??
        sqlcommand = "SELECT symbol, time, close FROM "+table_name+" WHERE symbol='"+symbol+"' ORDER BY time desc "
        cur.execute(sqlcommand)
        record = cur.fetchone()
        # Here, what if there are duplicate records???
        return record
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')


# update symbol
def Add_SymbolData(record, resolution, symbol, table_name):
    # set the start time is 60 seconds after the latest time stamp
    if record is not None:
        start_time = int(record[1]) + 60
        # Get current date as end_date
        end_time = util.convertDate_Unix(datetime.datetime.utcnow())
        # call Finnhub candle
        finnhub_client = finnhub.Client(api_key="bv4f2qn48v6qpatdiu3g")
        res = finnhub_client.stock_candles(symbol, resolution, int(start_time), int(end_time))
        time.sleep(1)
        # check if return value is no_data
        if res['s'] == 'no_data':
            print("The symbol " + symbol + " has no data")
        else:
            # remove stat column from df
            res.pop('s', None)

            df = pd.DataFrame(res)
            # insert symbol as first column in df
            df.insert(0, 'symbol', symbol, allow_duplicates=True)

            util.copyfrom_stringIO(df, table_name)
    else:
        print(symbol + "has no data")

if __name__ == '__main__':

    table_name='stock_daily'
    csv_file = 'sec_list_1000.csv'
    resolution = 'D'

    if check_Table(table_name):
        #symbols = pd.read_csv(csv_file, nrows=3).to_numpy()
        symbols = pd.read_csv(csv_file).to_numpy()
        #print(symbols)
        for symbol in symbols:

            record = retrieve_latestSymbol(symbol[0], table_name)
            #print(record)
            Add_SymbolData(record, resolution, symbol[0], table_name)

            # Update one minute data
            record = retrieve_latestSymbol(symbol[0], 'stock_minute')
            #print(record)
            Add_SymbolData(record, '1', symbol[0], 'stock_minute')






