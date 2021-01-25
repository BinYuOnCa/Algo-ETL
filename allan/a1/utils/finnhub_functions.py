import finnhub
import os
from datetime import datetime
from datetime import timedelta
from math import floor
import requests

# Gets start and end time for latest available timeframe given *timeframe* and time
def getStartEnd(timeframe, time):
    def truncToHour(time):
        return (time.year, time.month, time.day, time.hour)
    def myRound(x, base = 5):
        return base * floor(x/base)

    def prevTimeframe_60(time):
        return datetime(*truncToHour(time)) - timedelta(hours=1)

    def prevTimeframe_30(time):
        m = 0
        delta_h = 0
        if time.minute < 30:
            m = 30
            delta_h = 1
        return datetime(*truncToHour(time), m) - timedelta(hours=delta_h)

    def prevTimeframe_15(time):
        m = 30
        delta_h = 0
        if time.minute < 15:
            m = 45
            delta_h = 1
        else:
            m = myRound(time.minute, 15) - 15
        return datetime(*truncToHour(time), m) - timedelta(hours=delta_h)

    def prevTimeframe_5(time):
        m = 50
        delta_h = 0
        if time.minute < 5:
            m = 55
            delta_h = 1
        else:
            m = myRound(time.minute, 5) - 5
        return datetime(*truncToHour(time), m) - timedelta(hours=delta_h)

    def prevTimeframe_1(time):
        return datetime(*truncToHour(time), time.minute) - timedelta(minutes=1)

    timeframe = int(timeframe)
    if timeframe == 60:
        prevTimeframe = prevTimeframe_60 
    elif timeframe == 30:
        prevTimeframe = prevTimeframe_30
    elif timeframe == 15:
        prevTimeframe = prevTimeframe_15
    elif timeframe == 5:
        prevTimeframe = prevTimeframe_5
    elif timeframe == 1:
        prevTimeframe = prevTimeframe_1
    else:
        return "Nope"

    now = prevTimeframe(time)
    
    start = int(now.timestamp())
    end = int((now + timedelta(minutes=1)).timestamp())
    end = int((now + timedelta(minutes=timeframe - (1 if timeframe == 60 else 0))).timestamp())

    return {'start':start, 'end':end}

# Using API (No different than using lib since lib uses API)
def getCandleAPI(symbol, resolution, from_, to, sandbox=False):
    baseUrl = "https://finnhub.io/api/v1/"

    FINNHUB_KEY = os.getenv("FINNHUB_KEY")
    FINNHUB_KEY_SANDBOX = os.getenv("FINNHUB_KEY_SANDBOX")
    
    api_key = FINNHUB_KEY if not sandbox else FINNHUB_KEY_SANDBOX
    url = f"{baseUrl}/stock/candle?symbol={symbol}&resolution={resolution}&from={from_}&to={to}&token={api_key}"
    r = requests.get(url)

    if r.ok:
        return r.json()
    else:
        raise Exception(f"API get unsuccessful:\n{r.json()}")

