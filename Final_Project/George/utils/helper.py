import pandas as pd
from pathlib import Path
from datetime import datetime
import os
# from params import *

def write_df_to_csv_append(df, path=Path(__file__).parent / "../tmp/tmp.csv"):
    df.to_csv(path, mode='a', header=False)

def delete_csv(path=Path(__file__).parent / "../tmp/tmp.csv"):
    os.remove(path)


def get_last_timestamp(ticker, current_timestamp_df):
    try:
        last_timestamp = None
        last_timestamp = current_timestamp_df.loc[ticker, 'finn_timestamp']
        return last_timestamp
    except KeyError:
        return last_timestamp

def read_sec_list(file_name):
    return pd.read_csv(Path(__file__).parent / f"../config/{file_name}")['ticker'].tolist()

def market_opened(time_zone_adj, min_bfr=0):
    time = datetime.now().timetuple()
    return time


if __name__ == "__main__":

    print(market_opened(0))