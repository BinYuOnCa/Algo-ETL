from datetime import datetime
from .config import column_names, temp_csv


def candles_df_to_csv(df, interval):
    '''
    Reform df based on rds tables structure and write it to csv
    '''
    if interval == '1m':
        df['timestamp'] = df['t'].apply(
            lambda t: datetime.fromtimestamp(t).strftime('%H:%M:%S'))

    lag = 0 if interval == '1m' else 5 * 60 * 60

    df['t'] = df['t'].apply(
        lambda t: int(datetime.fromtimestamp(t + lag).strftime('%Y%m%d')))

    df = df.rename(
        {
            't': 'date_int_key',
            'o': 'open_price',
            'c': 'close_price',
            'h': 'high_price',
            'l': 'low_price',
            'v': 'volume'
        },
        axis='columns')
    # Reorder as RDS's structure
    new_index = column_names[interval]
    df = df[new_index]
    print(df)
    df.to_csv(temp_csv[interval], index=False, header=False)
