import finnhub
import pandas as pd

try:
    from message import error_sms
except ModuleNotFoundError:
    from .message import error_sms
try:
    from config import FINNHUB_CONFIG
except ModuleNotFoundError:
    from .config import FINNHUB_CONFIG

def connect_to_finnhub():
    try:
        finnhub_client = finnhub.Client(
            api_key=FINNHUB_CONFIG["FINNHUB_API"])
        print(finnhub_client)
        return finnhub_client
    except Exception:
        error_sms()
        raise

def download_1m_finn(stock,start_day,end_day):
    res = connect_to_finnhub().stock_candles(
        stock, '1', start_day, end_day )
    return res
    # stock_candles = pd.DataFrame.from_records(res)
    # print(stock_candles)

# download_1m_finn('BILI',1611211378,1611340978)

def download_daily_finn(stock,start_day,end_day):
    res = connect_to_finnhub().stock_candles(
        stock, 'D', start_day, end_day )
    print(res)
    return res