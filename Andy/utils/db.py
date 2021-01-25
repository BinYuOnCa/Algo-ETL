import os
import psycopg2
from .config import column_names, tab_names, temp_csv, log


class Database(object):
    def __init__(self, connection_string):
        '''
        initialize self's fields using connection_string
        :param connection_string: (Str)
        '''
        self.connection_string = connection_string

    def __enter__(self):
        '''
        enters the connection
        '''
        self.connector = psycopg2.connect(self.connection_string)
        self.cursor = self.connector.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        '''
        exits the connection with commit or rollback
        '''
        if exc_type is None:
            self.connector.commit()
        else:
            self.connector.rollback()
        self.connector.close()

    def ddl(self, sql):
        '''
        performs ddl
        :param sql: (Str)
        :return:
        '''
        self.cursor.execute(sql)

    def load_to_RDS(self, interval):
        '''
        uploads data into table corresponding to different interval
        from temporary csv file and remove it afterwards
        :param interval: 1m or 1d
        :return:
        '''
        with open(temp_csv[interval], 'r') as candles:
            self.cursor.copy_from(candles,
                                  tab_names[interval],
                                  columns=column_names[interval],
                                  sep=',')
        os.remove(temp_csv[interval])
