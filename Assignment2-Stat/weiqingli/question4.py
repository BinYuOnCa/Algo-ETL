import pandas as pd
import configparser
import lib.Tools as tools

config = configparser.ConfigParser()
config.read('./lib/application.config')

symbols = config['question4']['symbol'].split(',')
#print(symbols[3])
start_time = config['question2']['start_time']
end_time = config['question2']['end_time']
key = config['finnhub']['key']
resolution = config['question2']['resolution']

start = tools.date_to_epoch(start_time)
end = tools.date_to_epoch(end_time)

symbol_dict = {}

for symbol in symbols:

    res = tools.getStockCandles(key, symbol, resolution, start, end)
    res.pop('s', None)

    df = pd.DataFrame(res)

    # Insert Date column in df
    df.insert(0, "Date", "")
    # convert epotch time to datetime stamp in Date column
    df['Date'] = pd.to_datetime(df['t'], unit='s')
    df.insert(7, "daily_return", float, allow_duplicates=True)
    df1 = tools.cal_DailyReturn(df)
    symbol_dict[symbol] = df1


print(symbol_dict.get('AAPL'))

