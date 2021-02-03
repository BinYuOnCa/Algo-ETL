import pandas as pd
from utils.time_util import get_timestamp
from utils.config import finn_config


# Class of stocks
class Stock():
    def __init__(self, symbol):
        self.symbol = symbol.upper()

    def __str__(self):
        return self.symbol

    def __str__(self):
        profile_dict = finnhub_client.company_profile2(symbol=self.symbol)
        return 'Stock Symbol: {}\nCompany Name: {}\nExchange Market: {}\nIndustry: {}'.format(self.symbol, profile_dict['name'], profile_dict['exchange'], profile_dict['finnhubIndustry'])

    def fetchcandle(self, finn_client, res, start_time, end_time):
        # fetch candle data and return pandas dataframe
        res = finn_client.stock_candles(self.symbol, res, get_timestamp(start_time), get_timestamp(end_time))
        df = pd.DataFrame(res)
        return df

    def format_candle_df(self, df):
        # Clean candle dataframe
        if df[df['s'] != 'ok'].empty:
            df.drop('s', inplace=True, axis=1)
            df.columns = ['close', 'high', 'low', 'open', 'timestamp', 'volume']
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
            df['symbol'] = self.symbol
            return df
        else:
            # Later to add error notifications and better log info
            df_error = df[df['s'] == 'no_data']
            df_error.to_csv('./candle/error_log_' + self.symbol + '.csv', index_label='index')
            return 'Err'