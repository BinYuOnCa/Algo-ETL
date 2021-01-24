from utils.extract import download_candle
from datetime import datetime

x = download_candle('MMDEF',
                    '1m',
                    start_date=datetime(2021, 1, 1),
                    end_date=datetime(2021, 1, 24))
print(x)