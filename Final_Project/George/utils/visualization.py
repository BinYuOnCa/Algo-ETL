from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.dates as mpldates
import mplfinance as mpf


def candle_stick_daily(ticker, start_date=(datetime.now() - timedelta(days=60)).strftime("%Y%m%d"), end_date=datetime.now().strftime("%Y%m%d")):
    start_date = datetime.strptime(str(start_date), '%Y%m%d').strftime('%Y-%m-%d')
    end_date = datetime.strptime(str(end_date), '%Y%m%d').strftime('%Y-%m-%d')
    tkr = yf.Ticker(ticker)
    stock = tkr.history(interval="1d", start=start_date, end=end_date)
    stock.drop(columns=['Dividends', 'Stock Splits'], inplace=True)

    """
                      Open        High         Low       Close    Volume
Date                                                                
2021-01-20  858.739990  859.500000  837.280029  850.450012  25665900
2021-01-21  855.000000  855.719971  841.419983  844.989990  20521100
2021-01-22  834.309998  848.000000  828.619995  846.640015  20066500
2021-01-25  855.000000  900.400024  838.820007  880.799988  41173400
2021-01-26  891.380005  895.900024  871.599976  883.090027  23131600
    """
    print(f"plotting chart for {ticker} ...")
    # print(stock.head(5))
    fig_d = mpf.figure(style="yahoo")
    ax1_d = fig_d.add_subplot(1,1,1)
    mpf.plot(stock, ax=ax1_d, type='candle')
    mpf.show()
    pass


def candle_stick(ticker, start_date=(datetime.now() - timedelta(days=1)).strftime("%Y%m%d"), end_date=datetime.now().strftime("%Y%m%d")):
    start_date = datetime.strptime(str(start_date), '%Y%m%d').strftime('%Y-%m-%d')
    end_date = datetime.strptime(str(end_date), '%Y%m%d').strftime('%Y-%m-%d')
    tkr = yf.Ticker(ticker)
    stock = tkr.history(interval="1m", start=start_date, end=end_date)
    stock.drop(columns=['Dividends', 'Stock Splits'], inplace=True)
    print(f"plotting chart for {ticker} ...")
    # print(stock.head(5))
    fig_d = mpf.figure(style="yahoo")
    ax1_d = fig_d.add_subplot(1, 1, 1)
    mpf.plot(stock, ax=ax1_d, type='candle')
    mpf.show()
    pass


if __name__ == "__main__":
    # print(datetime.now().strftime("%Y-%m-%d"))
    # print((datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"))
    # # candle_stick_daily('TSLA')
    # candle_stick('TSLA', '2021-03-18', '2021-03-19')
    # # tkr = yf.Ticker('TSLA')
    # # stock = tkr.history(interval="1m", start='2021-03-18', end='2021-03-19')
    # # print(stock.head(5))
    pass