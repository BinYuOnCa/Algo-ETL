import psycopg2
import pyodbc
from pathlib import Path
import datetime
from dateutil.relativedelta import relativedelta


try:
    # print(Path(__file__).parent / "../logs/log.txt")
    conn = psycopg2.connect(
                                        host="algo-trade.cjnwlqlwl8gp.ca-central-1.rds.amazonaws.com",
        dbname="algo_trade",
                                        user="algorw", password="Toronto2020")


except psycopg2.OperationalError as e:
    with open(Path(__file__).parent / "../logs/db_log.log", "a") as f:
        f.write('"' + str(datetime.datetime.today()) +
                '", "There is an error with database connetion parameter", "' + str(e) + '"\n')
except psycopg2.ProgrammingError as e:
    with open(Path(__file__).parent / "../logs/db_log.log", "a") as f:
        f.write('"' + str(datetime.datetime.today()) +
                '", "There is an error with database connetions", "' + str(e) + '"\n')
except Exception as e:
    with open(Path(__file__).parent / "../logs/db_log.log", "a") as f:
        f.write('"' + str(datetime.datetime.today()) +
                '", "Something is wrong when executing the db_func", "' + str(e) + '"\n')



print(datetime.datetime.today()-relativedelta(months=24))