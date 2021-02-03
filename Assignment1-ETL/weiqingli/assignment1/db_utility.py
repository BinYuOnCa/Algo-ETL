import psycopg2
from configparser import ConfigParser
import datetime
import time
from datetime import timezone
from io import StringIO


# set up connection to local db
def config(filename='application.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

# Set up cursor in PostgreSQL
def cursor_setup():
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        # return current db connector
        return conn
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None



# Execute sql command to create table, insert multi rows, update row
def execute_sql(sqlCommand):
    conn = None
    try:
        conn = cursor_setup()
        # Create a cursor
        cur = conn.cursor()
        # create table one by one
        cur.execute(sqlCommand)
        # commit the changes
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally :
        if conn is not None :
            conn.close ()
            #print ( 'Database connection closed.')

def copyfrom_stringIO(df, table_name):
    try:
        conn = cursor_setup()
        cur = conn.cursor()
        #print(datetime.datetime.now())
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        # put the file position back at the start
        buffer.seek(0)

        cur.copy_from(buffer, table_name, sep=",")
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()




# Convert python timestamp to unix timestamp
def convertDate_Unix(dt):
    #dt = datetime.datetime(year, month, date, hour, minute)
    timestamp = dt.replace(tzinfo=timezone.utc).timestamp()
    return timestamp


def convertUnix_Date(unixtime):
    dt=datetime.datetime.utcfromtimestamp(int(unixtime))


    return dt


