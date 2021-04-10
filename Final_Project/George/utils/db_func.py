import urllib
from pathlib import Path
import datetime

import pyodbc
import psycopg2
from sqlalchemy import create_engine

import config.config_parser as conf



class DB_Conn:
    """
    Read from config file which db engine to use and initialize its relative connections.
    Return cooection or None (if engine is not mssql or postgresql or there is error
    """
    def __init__(self):
        self.db_engine = conf.settings()['db_engine']
        self.db_param = self.get_db_param()

    def get_db_param(self):
        if self.db_engine == "postgresql":
            return conf.postgresql()
        elif self.db_engine == "mssql":
            return conf.mssql()
        else:
            return None

    def create_connection(self):
        try:
            if self.db_engine == "postgresql":
                return psycopg2.connect(
                                        host=str(self.db_param["host"]), dbname=str(self.db_param["db"]),
                                        user=str(self.db_param["user"]), password=str(self.db_param["passwd"]))
            elif self.db_engine == "mssql":
                return pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; '
                                      'SERVER=' + str(self.db_param["host"]) + ';DATABASE=' + str(self.db_param["db"]) +
                                      ';UID=' + str(self.db_param["user"]) + ';PWD=' + str(self.db_param["passwd"]))
            else:
                return None

        except pyodbc.InterfaceError as e:
            with open(Path(__file__).parent / "../logs/db_log.log", "a") as f:
                f.write('"' + str(datetime.datetime.today()) +
                        '", "There is an error with database connetion parameter", "' + str(e) + '"\n')
        except pyodbc.OperationalError as e:
            with open(Path(__file__).parent / "../logs/db_log.log", "a") as f:
                f.write('"' + str(datetime.datetime.today()) +
                        '", "There is an error with database connetions", "' + str(e) + '"\n')
        except pyodbc.ProgrammingError as e:
            with open(Path(__file__).parent / "../logs/db_log.log", "a") as f:
                f.write('"' + str(datetime.datetime.today()) +
                        '", "There is a programming error with database connetions", "' + str(e) + '"\n')
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


    def create_sqlalchemy_engine(self):
        try:
            if self.db_param is not None:
                if self.db_engine == "postgresql":
                    # setup postgreSQL server
                    return create_engine(
                        "postgresql+psycopg2://{user}:{pw}@{host}/{db}".format(
                            user=str(self.db_param["user"]), pw=str(self.db_param["passwd"]),
                            host=str(self.db_param["host"]), db=str(self.db_param["db"])))
                elif self.db_engine == "mssql":
                    # setup SQL server
                    conn_string = 'DRIVER={ODBC Driver 17 for SQL Server}; ' \
                                  'SERVER=' + str(self.db_param["host"]) + ';DATABASE=' + str(self.db_param["db"]) + \
                                  ';UID=' + str(self.db_param["user"]) + ';PWD=' + str(self.db_param["passwd"])
                    return create_engine(
                        "mssql+pyodbc:///?odbc_connect=%s" % urllib.parse.quote_plus(conn_string), fast_executemany=True)
                else:
                    return None
            else:
                return None
        except Exception as e:
            with open(Path(__file__).parent / "../logs/db_log.log", "a") as f:
                f.write('"' + str(datetime.datetime.today()) +
                        '", "Something is wrong when executing the db_func", "' + str(e) + '"\n')


def connect_to_db():
    db_conn = DB_Conn()
    return db_conn.create_connection()




if __name__ == "__Main__":
    pass
