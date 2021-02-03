import pytest
import psycopg2
from configparser import ConfigParser
import UpdateStockData as updatestockdata
import db_utility as utility
import datetime

# Verify the time stamp of symbol daily is equal to stock split time
def test_StockUpdate():
    table_name = 'stock_daily'
    symbol = 'MGNX'
    split_time = dt = datetime.datetime(2021, 1, 21, 14, 30)
    split_time_epoch = utility.convertDate_Unix(split_time)

    record = updatestockdata.retrieve_latestSymbol(symbol, table_name)
    time = int(record[1])

    assert time == split_time_epoch




