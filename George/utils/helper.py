import pandas as pd
from pathlib import Path
import os


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