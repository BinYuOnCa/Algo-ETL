def stock_candles(api_key, symbol, start_time, end_time, resolution='D'):
    """
    get Stock candles as a dataframe from symbols using finnhub-python
    :param api_key:string
    :param start_time: timestamp int
    :param end_time: timestamp int
    :param resolution: string Supported resolution includes 1, D, W, M. Default D
    :return: candles_df as a pd.dataframe or None
    """
    import finnhub
    import pandas as pd
    import time
    import requests

    #candles_df = pd.DataFrame(columns=['c', 'h', 'l', 'o', 't', 'v', 'symbol'])
    finnhub_client = finnhub.Client(api_key=api_key)
    time.sleep(1)
    try:
        res = finnhub_client.stock_candles(
            symbol, resolution, start_time, end_time)
        # only put records with status in 's'
        if res.get('s') == 'ok':
            candles_df = pd.DataFrame.from_dict(res).drop(columns='s')
            # rename columns for the db table
            candles_df.columns = [
                'close',
                'high',
                'low',
                'open',
                'date',
                'volume']
            candles_df['symbol'] = symbol
            candles_df['volume'] = candles_df['volume'].astype(int)
            candles_df['date'] = pd.to_datetime(candles_df['date'], unit='s')
            return candles_df

    except (requests.exceptions.RequestException, ValueError) as e:
        time.sleep(120)
        pass


def candles(api_key, symbols, start_time=None, resolution='D', end_time=None):
    """
    get Stock candles as a dataframe from symbols using finnhub-python
    :param api_key:string
    :param start_time: timestamp int. if none then most recent time in the DB
    :param end_time: timestamp int. if none then current time
    :param resolution: string Supported resolution includes 1, D, W, M. Default D
    :return: candles_df as a pd.dataframe
    """
    import pandas as pd
    import configparser
    import utils.insert_tb as insert_tb
    import time
    from datetime import datetime

    config = configparser.ConfigParser()
    config.read('./settings.ini')
    max_data_len = int(config['finnhub']['max_data_len'])
    max_gap = 60 * 86400  # API can return <= 60 days data per query

    candles_df = pd.DataFrame()
    candles_df1 = pd.DataFrame()
    if end_time is None:
        end_time = int(time.time())

    if resolution not in ['1', 'D']:
        raise Exception("not supported this resolution:", resolution)

    if resolution == '1':
        tablename = 'us_equity_1_finn'
        startdt_min = int(
            datetime.strptime(
                config['finnhub']['startdt_min'],
                '%Y-%m-%d').timestamp())
        for ind, symbol in symbols.iterrows():
            # get the most recent time from records in db
            if start_time is None:
                most_recent = insert_tb.most_recent(symbol[0], tablename)
                start = most_recent
                if most_recent == 0:
                    start = startdt_min
            else:
                start = start_time
            time_gap = end_time - start
            #print(start, end, symbol[0])
            if time_gap <= max_gap:  # not large than 60 days
                data = stock_candles(
                    api_key,
                    symbol[0],
                    start_time=start,
                    resolution=resolution,
                    end_time=end_time)
                if data is not None:
                    candles_df = pd.concat(
                        [candles_df, data], ignore_index=True)

            else:  # using while loop every 60days
                end = start + max_gap
                while start < end_time:
                    # print(start,end,start_time,end_time)
                    data = stock_candles(
                        api_key,
                        symbol[0],
                        start_time=start,
                        resolution=resolution,
                        end_time=end)
                    start += max_gap
                    if start + max_gap < end_time:
                        end = start + max_gap
                    else:
                        end = end_time
                    if data is not None:
                        candles_df1 = pd.concat(
                            [candles_df1, data], ignore_index=True)
                        if candles_df1.shape[0] > max_data_len:
                            #print(candles_df1.shape[0], symbol[0], '1')
                            insert_tb.copy_from(
                                candles_df1, tablename)  # copy_from
                            # empty content of the dataframe
                            candles_df1.drop(candles_df1.index, inplace=True)

    elif resolution == 'D':
        tablename = 'us_equity_daily_finn'
        startdt_day = int(
            datetime.strptime(
                config['finnhub']['startdt_day'],
                '%Y-%m-%d').timestamp())
        for ind, symbol in symbols.iterrows():
            # get the most recent time from records in db
            if start_time is None:
                most_recent = insert_tb.most_recent(symbol[0], tablename)
                start = most_recent
                if most_recent == 0:
                    start = startdt_day
            else:
                start = start_time
            data = stock_candles(
                api_key,
                symbol[0],
                start_time=start,
                resolution=resolution,
                end_time=end_time)
            #print(start, end_time, symbol[0])
            if data is not None:  # if data.shape[0] > 0:
                candles_df = pd.concat([candles_df, data], ignore_index=True)
                if candles_df.shape[0] > max_data_len:
                    #print(candles_df.shape[0], symbol[0], 'd2')
                    insert_tb.copy_from(candles_df, tablename)  # copy_from
                    # empty content of the dataframe
                    candles_df.drop(candles_df.index, inplace=True)

    if candles_df.shape[0] > 0:
        #print(candles_df.shape[0], symbol[0], 'd1/0')
        insert_tb.refresh(candles_df, tablename)


def split_df(symbols, api_key, start_date=None, end_date=None):
    """
    get Stock split as a dataframe from symbols using finnhub-python
    :param api_key:string
    :param start_date: string default is 2 days ago
    :param end_date: string default today
    :param resolution: string Supported resolution includes 1, D, W, M. Default D
    :return: split_df as a pd.dataframe
    """
    import finnhub
    import pandas as pd
    import time
    from datetime import date, timedelta

    running_time = time.perf_counter()
    split_df = pd.DataFrame(
        columns=[
            'symbol',
            'date',
            'fromFactor',
            'toFactor'])
    finnhub_client = finnhub.Client(api_key=api_key)
    #quaries = 0
    #running_start = time.perf_counter()
    if end_date is None:
        end_date = date.today().strftime('%Y-%m-%d')
    if start_date is None:
        start_date = (date.today() - timedelta(2)).strftime('%Y-%m-%d')

    # ilterate stocks
    for ind, symbol in symbols.iterrows():
        # limit 60 API calls/minute
        time.sleep(1)
        split = finnhub_client.stock_splits(
            symbol[0], _from=start_date, to=end_date)
        if len(split) > 0:
            split_df = pd.concat(
                [split_df, pd.DataFrame.from_dict(split)], ignore_index=True)

    if split_df.shape[0] > 0:
        split_df['date'] = pd.to_datetime(split_df['date'], format='%Y-%m-%d')
    return split_df
