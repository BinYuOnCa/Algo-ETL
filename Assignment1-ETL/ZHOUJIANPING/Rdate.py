import finnhub
import numpy as np
import pandas as pd
from datetime import datetime


def timestamps_time2(df_D):
    df_D['Time_ymd']=np.nan
    df_D['Time_HMS']=np.nan
    for i in range(len(df_D.iloc[:, 5])):
        df_D['Time_ymd'][i]=datetime.utcfromtimestamp(df_D['timestamp'][i]).strftime('%Y-%m-%d')
        df_D['Time_HMS'][i]=datetime.utcfromtimestamp(df_D['timestamp'][i]).strftime('%H:%M:%S')
        
def df_finhub(Sysblem,timestap,startdate,enddate):
    finnhub_client = finnhub.Client(api_key='c01p71n48v6ofkrk9460')
    res = finnhub_client.stock_candles(Sysblem, timestap, startdate, enddate)
    df_D=pd.DataFrame(res)
    df_D=df_D.rename(columns={"c": "close_price", "h": "high_price","l": "low_price", "o": "open_price","t": "timestamp", "v": "volume"})
    return df_D
