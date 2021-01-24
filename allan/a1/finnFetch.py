import datetime as dt
from datetime import datetime
import finnhub
from psycopg2.errors import UndefinedTable
from io import StringIO
import pandas

import config # Needed before other functions
import utils.finnhub_functions as finn_fnc
import utils.psql_functions as psql_fnc
from utils.log_functions import print_log

class FinnFetch:

    def __init__(self):
        self.finnClient = finn_fnc.setupClient(config.use_sandbox) 
        self.conn = psql_fnc.setupConn(config.use_sandbox)
        self.cur = self.conn.cursor()

    #def check_tables(self, ):

    # Fetch and load for each symbol
    def fetchAndInsert_candle(self, params):
        data = self.finnClient.stock_candles(*params)
        
        # params is in shape of ('symbol', 'timeframe', 'start', 'end')

        # Error handling
        if data['s'] != 'ok' or data['c'] == None:
            print_log(f">>>>> No Data >>>>>\n No data for params: {params}")
            return None

        table_name = f"{params[0]}_candle_{params[1]}"
        if len(data['c']) > 1:
            return self.bulkInsert_candle(data, table_name)
        else:
            return self.singleInsert_candle(data, table_name)

    # Default behavior tries to create table beforehand
    def bulkInsert_candle(self, data, table_name, nocreate=False):
        df = pandas.DataFrame(data)
        df['t'] = pandas.to_datetime(df['t'], unit='s')

        csv_data = df.to_csv(
                header=False,
                columns=['t', 'o', 'h', 'l', 'c', 'v'],
                index=False)

        # Copy into table
        f = StringIO(csv_data)
        try:
            if not nocreate:
                self.createTable_candle(table_name)
            self.cur.copy_from(f, table_name, sep=',')
        except Exception as e:
            print_log(f">>>>> DB_ERROR >>>>>\n{e}\n>>>>>>>>>>>>>>>>>>>")
            return self.__psql_ret(False, e)

        print_log(f"Bulk insert successful\n  Inserted {len(df.index)} entries into {table_name}")
        return self.__psql_ret(True)
        

    def singleInsert_candle(self, data, table_name):
        t = data['t'][0]
        o = data['o'][0]
        h = data['h'][0]
        l = data['l'][0]
        c = data['c'][0]
        v = data['v'][0]

        insert_statement = f"""INSERT INTO {table_name} (
        timestamp, open, high, low, close, volume
    ) VALUES (
        (SELECT TO_TIMESTAMP({t})), {o}, {h}, {l}, {c}, {v}
    )"""
        try:
            self.cur.execute(insert_statement)
        except UndefinedTable as e:     # If table DNE, try to create
            print_log(f"Undefined table \"{table_name}\"\b Attempting to create it")
            try:
                self.commit()
                self.createTable_candle(table_name)
                self.cur.execute(insert_statement)

            except Exception as e:
                print_log(f">>>>> DB_ERROR >>>>>\n{e}\n>>>>>>>>>>>>>>>>>>>>")
                return self.__psql_ret(False, e)

            #self.cur.execute(insert_statement)
            return self.__psql_ret(True, "Created {table_name}")
        except Exception as e:
            print_log(f">>>>> DB_ERROR >>>>>\n{e}\n>>>>>>>>>>>>>>>>>>>>")
            return self.__psql_ret(False, e)

        print_log("Insert successful")
        return self.__psql_ret(True)

    def createTable_candle(self, table_name):
        try:
            self.cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp timestamp,
            open float,
            high float,
            low float,
            close float,
            volume int)""")
            self.commit()
        except Exception as e:
            print_log(f">>>> DB_ERROR >>>>>\n Unable to create table {table_name}.")
            return self.__psql_ret(False, e)

        print_log(f"Created table (IF NOT EXISTS) {table_name}")
        return self.__psql_ret(True)

    def commit(self):
        self.conn.commit()

    def __psql_ret(self, success, msg=""):
        return dict(success=success, msg=msg)

