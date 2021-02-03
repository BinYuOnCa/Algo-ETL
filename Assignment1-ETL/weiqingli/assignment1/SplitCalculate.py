import pandas as pd
from io import StringIO
import psycopg2
import finnhub
import db_utility as util
import logging
import time
import datetime


# Query symbol from daily table and write to a dataframe
def ConvertQuery_DF(symbol,  table_name):
    sqlcommand = "SELECT * FROM "+table_name+" WHERE symbol = '"+symbol+"'"
    try:
        conn = util.cursor_setup()
        # Create a cursor
        cur = conn.cursor()
        # create table one by one
        cur.execute(sqlcommand)
        df = pd.read_sql(sqlcommand, conn)
        #df.insert(0, 'symbol', symbol, allow_duplicates=True)

        # commit the changes
        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally :
        if conn is not None :

            conn.close ()
            #print ( 'Database connection closed.')
        return df


# Reculate close, high, low, open prices
def Recalculate_Stock(split_ratio, df):

    ratio = 1 / split_ratio
    df['close'] = df['close'] * ratio
    df['high'] = df['high'] * ratio
    df['low'] = df['low'] * ratio
    df['open'] = df['open'] * ratio


    return df


symbol = 'MGNX'
split_ratio = 2
start_time  = 979527600
end_time = util.convertDate_Unix(datetime.datetime.utcnow())
resolution = 'D'
dailytable = 'stock_daily'
df = ConvertQuery_DF(symbol, dailytable)
df = Recalculate_Stock(split_ratio, df)

# Delete records from table
sqlcommand = "DELETE FROM "+dailytable+" WHERE symbol = '"+symbol+"'"
util.execute_sql(sqlcommand)
# Insert the reculated dataframe into table
util.copyfrom_stringIO(df, dailytable)

#print("END")




