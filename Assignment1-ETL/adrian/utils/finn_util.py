import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
from utils.config import finn_config

base_url = 'https://finnhub.io/api/v1'
headers = {'X-Finnhub-Token': finn_config['finn_api']}


# Create retry and timeout strategy for bad internet connection
retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    backoff_factor=1
)
adapter = HTTPAdapter(max_retries=retry_strategy)
s = requests.Session()
s.mount("https://", adapter)

def get_stock_candle(symbol, res, from_ts, to_ts, format_='json'):
    sec_url = '/stock/candle'
    params = {
        'symbol': symbol,
        'resolution': res,
        'from': from_ts,
        'to': to_ts,
        'format': format_
    }
    res = s.get(base_url + sec_url, headers=headers, params=params, timeout=(3,15))
    if format_ == 'json':
        candle_dict = res.json()
        if candle_dict['s'] == 'ok':
            # print('Data acquired successfully.')
            df = pd.DataFrame(res.json())
        elif candle_dict['s'] == 'no_data':
            # print('No data in the requested range.')
            df = None
    elif format_ == 'csv':
        file_path = symbol + '.csv'
        with open(file_path, 'w') as f:
            f.write(res.text)
        df = None
    return df


def get_company_profile2(finn_client, symbol):
    '''
    Using requests module
    
    sec_url = '/stock/profile2'
    params = {'symbol': symbol}
    res = s.get(base_url + sec_url, headers=headers, params=params, timeout=(5,10))
    return res.json()
    
    Using finnhub API below
    '''
    return finn_client.company_profile2(symbol=symbol)


def format_candle_df(df, symbol):
    if df[df['s'] != 'ok'].empty:
        df.drop('s', inplace=True, axis=1)
        df.columns = ['close', 'high', 'low', 'open', 'timestamp', 'volume']
        df['volume'] = df['volume'].astype('int64')
        df['symbol'] = symbol
        datetime_series = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize('utc').dt.tz_convert('US/Eastern')
        df['date_key_int'] = datetime_series.dt.strftime('%Y%m%d')
        df['time_key'] = datetime_series.dt.strftime('%H:%M:%S')
        # df.drop('timestamp', inplace=True, axis=1)
    else:
        # Need more code to deal with exception
        df = None
    return df


if __name__ == '__main__':
    from utils.time_util import usest_str_to_ts
    import time
    """     # print(get_company_profile2('tsla'))
    t0 = time.time()
    df = get_stock_candle('TSLA', '1', usest_str_to_ts('20201001'),
                          usest_str_to_ts('20201130'))
    df2 = format_candle_df(df, 'TSLA')
    print(df2.info())
    t2 = time.time()
    print(f'Total records fetched: {df2.shape[0]}. Total time used: {t2-t0}.')
    # df2.to_csv('apt_1m2.csv')
    """
    ts0 = usest_str_to_ts('20200101 00:00:00')
    ts1 = usest_str_to_ts('20200131 23:59:59')
    df = get_stock_candle('AMZN', '1', ts1, ts0)
    # df2 = format_candle_df(df, 'AMZN')
    print(df)
