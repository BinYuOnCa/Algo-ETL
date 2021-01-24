import os
import psycopg2

from log_functions import print_log

def setupConn(sandbox=False):
    ENDPOINT = os.getenv("RDS_ENDPOINT")
    PORT = os.getenv("RDS_ENDPOINT_PORT")
    USR = os.getenv("RDS_USERNAME")
    DBNAME = os.getenv("RDS_DBNAME_SANDBOX" if sandbox else "RDS_DBNAME")
    PASS = os.getenv("RDS_PASSWORD")

    print_log(f"CONNECTING TO {DBNAME} as {USR}")
    # Try to connect
    try:
        conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USR, password=PASS)
    except Exception as e:
        print_log("I am unable to connect to the database.")
        print_log(f"Caught exception: {e}")

        return None
    
    print_log("CONNECTION Established")
    return conn

