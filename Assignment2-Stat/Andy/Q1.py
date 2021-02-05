import yfinance as yf
import plotly.graph_objs as go

# Download Candles Data
candles_df = yf.Ticker('THRM.V').history(interval='1m',
                                         start='2021-1-29',
                                         end='2021-1-30')
candles_df.reset_index(inplace=True)

# Calculate VWAP
avg_price = candles_df[['High', 'Low', 'Close']].mean(axis=1)
cum_price_vol = (avg_price * candles_df['Volume']).cumsum()
cum_vol = candles_df['Volume'].cumsum()
candles_df['VWAP'] = (cum_price_vol / cum_vol)

#  Plot
fig = go.Figure()
# Candlesticks Plot
fig.add_trace(
    go.Candlestick(x=candles_df['Datetime'],
                   open=candles_df['Open'],
                   high=candles_df['High'],
                   low=candles_df['Low'],
                   close=candles_df['Close'],
                   name='Candlesticks Plot'))
# VWAP Plot
fig.add_trace(
    go.Scatter(x=candles_df['Datetime'],
               y=candles_df['VWAP'],
               marker=dict(color='blue'),
               name='VWAP'))

fig.update_layout(
    title="THRM.V Intraday Candlestick & VWAP Plot on 2021/1/29",
    xaxis_title="Time",
    yaxis_title="Price",
    legend_title='Plot Type',
)

fig.show()
