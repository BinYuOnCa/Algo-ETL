"""Assignment 2:
1.	Get 1-day trading data (open, close, high, low and volume) with 1 minute interval for THRM.
Plot candle stick plot and VWAP in one figure using the 1-day data above.  [20 points]
(10 points for candle stick plot and 10 points for VWAP)
"""
import stock_assginment2_utils as utils


symbol = "THRM"
from_t = "2020-02-03 09:30:00"
to_t = "2020-02-03 16:00:00"
"""
Get 1-day trading data (open, close, high, low and volume) with 1 minute interval for THRM
"""
sql = f"select o,c,h,l,v from stock_candles_min where symbol = '{symbol}' and dt>='{from_t}' and dt<='{to_t}' "
dt = utils.query_dt(sql)
print(dt)

"""
VWAP in one figure using the 1-day data above
"""
utils.vwap_pic(symbol, from_t, to_t)

"""
Plot candle stick plot
方式1
"""
utils.candle_stick_plot1(symbol, from_t, to_t)

"""
Plot candle stick plot
方式2
"""
utils.candle_stick_plot2(symbol, from_t, to_t)