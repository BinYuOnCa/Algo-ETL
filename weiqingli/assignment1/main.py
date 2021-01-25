import UpdateStockData as updatestockdata
import InitialData as initialdata
import db_utility as util
import logging
import finnhub
import datetime
import pandas as pd
import MessageMe as message


logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
logger = logging.getLogger()

# Setup finnhub client
dailytable = 'stock_daily'
minutetable = 'stock_minute'
finnhub_client = finnhub.Client ( api_key="bv4f2qn48v6qpatdiu3g" )

#print("Here is a")
if updatestockdata.check_Table('stock_daily') is False:
    sqlcommand = "CREATE TABLE IF NOT EXISTS " + dailytable + " (" \
                                                              "symbol   VARCHAR(50) NOT NULL, " \
                                                              "close    FLOAT NOT NULL, " \
                                                              "high     FLOAT NOT NULL, " \
                                                              "low      FLOAT NOT NULL, " \
                                                              "open     FLOAT NOT NULL, " \
                                                              "time     INT NOT NULL, " \
                                                              "volume   FLOAT NOT NULL) "

    util.execute_sql(sqlcommand)
    initialdata.initial_stockdata('sec_list_1000.csv', 'D', 979527600, 1610582400, dailytable)
    logging.info('Create the database table '+ dailytable)

if updatestockdata.check_Table('stock_minute') is False:
    sqlcommand = "CREATE TABLE IF NOT EXISTS " + minutetable + " (" \
                                                               "symbol   VARCHAR(50) NOT NULL, " \
                                                               "close    FLOAT NOT NULL, " \
                                                               "high     FLOAT NOT NULL, " \
                                                               "low      FLOAT NOT NULL, " \
                                                               "open     FLOAT NOT NULL, " \
                                                               "time     INT NOT NULL, " \
                                                               "volume   FLOAT NOT NULL) "

    util.execute_sql(sqlcommand)
    initialdata.initial_stockdata('sec_list_1000.csv', '1', 979527600, 1610582400, minutetable)
    logging.info('Create the database table '+ minutetable)


csv_file = 'sec_list_1000.csv'

#symbols = pd.read_csv(csv_file, nrows=3).to_numpy()
symbols = pd.read_csv(csv_file).to_numpy()
#print(symbols)
for symbol in symbols:
    record = updatestockdata.retrieve_latestSymbol(symbol[0], dailytable)
    # print(record)
    updatestockdata.Add_SymbolData(record, 'D', symbol[0], dailytable)

    # Update one minute data
    record = updatestockdata.retrieve_latestSymbol(symbol[0], 'stock_minute')
    # print(record)
    updatestockdata.Add_SymbolData(record, '1', symbol[0], 'stock_minute')

message.message_me()