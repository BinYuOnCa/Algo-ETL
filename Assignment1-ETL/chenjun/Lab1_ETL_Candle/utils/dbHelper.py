import psycopg2
from datetime import datetime
from utils.dbConfig import config
import datetime as dt

def getLastUpdateTimeStamp (conn, table, ticker):
    cursor = conn.cursor()
    lastTime_Stamp = None
    try:
        cursor.execute("select time_stamp from " + f"{table}" + " where symbol='" + f"{ticker}" + "' order by id_id DESC LIMIT 1 ")
        print("select time_stamp from " + f"{table}" + " where symbol='" + f"{ticker}" + "' order by id_id DESC LIMIT 1")
        lastTime_Stamp_db = cursor.fetchone()
        lastTime_Stamp=lastTime_Stamp_db[0]
        print(lastTime_Stamp)
        print(lastTime_Stamp_db[0].strftime("%Y-%m-%d %H:%M:%S"))
        #lastTime_Stamp = lastTime_Stamp_db[0].strftime("%Y-%m-%d %H:%M:%S")
        #print("Last updated Time Stamp from " + table + " for stock=" + ticker + " is: " + lastTime_Stamp)

    except (Exception, psycopg2.DatabaseError) as error:
        if lastTime_Stamp is None:
            print("no last update timestamp found")
        print("Error: %s" % error)
    finally:
        cursor.close()
        if lastTime_Stamp is not None:
            return lastTime_Stamp
        else:
            return datetime(2020,1,1,0,0)


def get_next_start_id(conn, table):
    cursor = conn.cursor()
    try:
        cursor.execute("select count(id_id) from " + f"{table}")
        recordsCount = cursor.fetchone()
        if recordsCount[0]==0:
            print("initialization.....")
            return get_next_val(conn, table)
        cursor.execute("SELECT max(id_id) from " + f"{table}")
        next_start_id = cursor.fetchone()
        print("found existing largest ID.....")
        return next_start_id
    except (Exception, psycopg2.DatabaseError) as error:
        if next_start_id is None:
            print("empty table")
        print("Error: %s" % error)
        cursor.close()
        return 1
        print("fetched starting ID is done")
    cursor.close()

def get_next_val(conn, table):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT nextval('idid')")
        nextval = cursor.fetchone()
        return nextval
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        # conn.rollback()
        cursor.close()
        return 1
        print("fetched next-val is done")
        cursor.close()

def get_db_connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            return conn
            #cj: conn.close()
            #cj: print('Database connection closed.')


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('test Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Connection is successful - Database connection closed.')