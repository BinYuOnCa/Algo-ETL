import pandas as pd

import lib.finnhub_wrap
import lib.db_wrap


def load_and_save(symbol):
    try:
        df = lib.finnhub_wrap.get_stock_day_candles(symbol)
        df['symbol'] = symbol
        df['t'] = pd.to_datetime(df['t'], unit='s')
        if df.empty:
            print('>>>', symbol, 'no_data')
            return
        df.to_sql('finnhub_us_day_candle', lib.db_wrap.get_engine(), if_exists='append', index=False)
    except Exception as e:
        print('>>>', symbol, f'error:{e}')
        return
    print('>>>', symbol, f'{len(df)}')


us_symbols = pd.read_sql('select symbol from finnhub_us_symbols', lib.db_wrap.get_engine())
us_symbols_saved = pd.read_sql('select distinct symbol from finnhub_us_day_candle', lib.db_wrap.get_engine())
symbols = set(us_symbols['symbol'])-set(us_symbols_saved['symbol'])
print(f'total {len(symbols)} symbols need to be download')
for s in symbols:
    load_and_save(s)
