#-*-coding:utf-8-*-
import time
from utils.stock_download_import import Etl
from execution.stock_candles_min import Minute_1


"""
This is a main program, only execute candles_day and then candles_min
"""
begin_time = int(time.time())
dt = Etl("day")
dt.stock_download_import()
end_time = int(time.time())
print(f"This task spend  {round((end_time-begin_time)/60,2)} minutes.")


begin_time = int(time.time())
# Inherit the parent class
dt = Minute_1("1min")
dt.stock_download_import()
end_time = int(time.time())
print(f"This task spend  {round((end_time - begin_time) / 60, 2)} minutes.")
