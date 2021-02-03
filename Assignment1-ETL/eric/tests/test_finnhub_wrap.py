import lib.finnhub_wrap
import util.measuretime

def run():
    print('Test1: Get all day candle data of "AAPL".')
    with util.measuretime.MeasureTime() as t:
        df = lib.finnhub_wrap.get_stock_day_candles('AAPL')
        print(df.describe())
    print(f'Elapsed time: {t}s')

    print()
    print('Test2: Get all 1min candle data of "AAPL".')
    with util.measuretime.MeasureTime() as t:
        df = lib.finnhub_wrap.get_stock_1min_candles('AAPL')
        print(df.describe())
    print(f'Elapsed time: {t}s')

    print()
    print('Test3: Get all day candle data of "GGAL".')
    with util.measuretime.MeasureTime() as t:
        df = lib.finnhub_wrap.get_stock_day_candles('GGAL')
        print(df.describe())
        print(df)
        print('Duplicated rows', df[df.duplicated(['symbol', 't'], keep=False)])
    print(f'Elapsed time: {t}s')

    return True
