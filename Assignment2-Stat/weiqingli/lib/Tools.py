import finnhub
import datetime
import math
import plotly.graph_objs as go
import numpy as np

def date_to_epoch(time_input):
    dt = datetime.datetime.strptime(time_input, "%Y-%m-%d")
    return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())

def getStockCandles(key, symbol, resolution, start_time, end_time):
    # setup finnhub client
    finnhub_client = finnhub.Client(api_key=key)
    # retrieve stock candle
    res = finnhub_client.stock_candles(symbol, resolution, start_time, end_time)
    return res


def cal_EMA(period, df, column_name):
    total_initial = 0
    for index in range(0, period):
        total_initial = total_initial + df.at[index, 'c']
        df.at[index, column_name] = 0.0
    avg_period = total_initial / period
    multiplier = 2 / (period + 1)
    for index in range(period - 1, len(df)):
        if index < period:
            df.at[index, column_name] = avg_period
        else:
            df.at[index, column_name] = df.at[index, 'c'] * multiplier + df.at[index - 1, column_name] * (1 - multiplier)

    return df

# return = log(today close / previous close)
def cal_DailyReturn(df):
    for index in df.index:
        if index == 0:
            df.at[index, 'daily_return'] = math.log(df.at[index, 'c']/df.at[index, 'c'])
        else:
            df.at[index, 'daily_return'] = math.log(df.at[index, 'c']/df.at[index-1, 'c'])
    return df


# draw candle stick
def drawCandleStick_ByPlotly(df):
    df.reset_index(inplace=True)
    fig = go.Figure(data=[go.Candlestick(x=df['Date'], open=df['o'], high=df['h'], low=df['l'], close=df['c'])])
    fig.show()


