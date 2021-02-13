import yfinance as yf
import plotly.graph_objs as go
from statistics import mean


# function to return exponential moving average
def EMA(period, close_price):
    '''
    return list of exponential moving average given period
    '''
    multiplier = (period + 1)**(-1)
    res = []
    for index, close_p in enumerate(close_price):
        if index >= period - 1:
            ema_prev = res[-1] if res else mean(close_price[:index + 1])
            ema_curr = multiplier * close_p + (1 - multiplier) * ema_prev
            res.append(ema_curr)
    return [None] * (period - 1) + res


# Download
candles_df = yf.Ticker('THRM.V').history(interval='1d',
                                         start='2018-1-1',
                                         end='2021-1-1')
candles_df.reset_index(inplace=True)

# EMA
candles_df['EMA_30'] = EMA(30, candles_df['Close'])
candles_df['EMA_100'] = EMA(100, candles_df['Close'])

# Plot
fig = go.Figure()
# Candlesticks Plot
fig.add_trace(
    go.Candlestick(x=candles_df['Date'],
                   open=candles_df['Open'],
                   high=candles_df['High'],
                   low=candles_df['Low'],
                   close=candles_df['Close'],
                   name='Candlesticks Plot'))
# 30-day EMA Plot
fig.add_trace(
    go.Scatter(x=candles_df['Date'],
               y=candles_df['EMA_30'],
               marker=dict(color='blue'),
               name='30-day EMA'))
# 100-day EMA Plot
fig.add_trace(
    go.Scatter(x=candles_df['Date'],
               y=candles_df['EMA_100'],
               marker=dict(color='purple'),
               name='100-day EMA'))

fig.update_layout(
    title="THRM.V Daily Candlesticks & EMA Plot from 2018/1/1 to 2020/12/31",
    xaxis_title="Time",
    yaxis_title="Price",
    legend_title="Plot Type")

fig.show()
