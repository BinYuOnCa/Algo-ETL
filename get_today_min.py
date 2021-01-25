import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import os
import psycopg2
from io import StringIO
from send_sms import send_msg


# Initiate an database instance
def db_connect():
    conn = psycopg2.connect(database=os.environ['DB_DB'],
                            user=os.environ['DB_USERNAME'],
                            password=os.environ['DB_PASSWORD'],
                            host=os.environ['DB_HOST'],
                            port=os.environ['DB_PORT']
                            )
    return conn


def get_stock_min(resolution):

    # Retry on Failure for finnhub API call limit
    retry_strategy = Retry(
        total=20,
        status_forcelist=[429],
        method_whitelist=["GET"],
        backoff_factor=1
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    # Connect to an existing databse
    conn = db_connect()
    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Setup client
    api_key = os.environ['API_KEY']

    # Stock symbol list
    symbols = pd.read_csv('/home/ubuntu/luciexie-A1/companylist.csv')
    symbols = symbols['Symbol']

    # Setup time
    # Initializing the past one year minute dataset,
    # due to the finnhub API restriction
    # a loop through the past 12 months is needed at the first time
    # one_yr = relativedelta(years = 1)
    # one_month = relativedelta(months = 1)
    # start = (datetime.now() - one_yr).timestamp()
    # end = start

    # To get the past one day daily data
    one_day = relativedelta(hours=24)
    end = datetime.now().timestamp()
    start = (datetime.now() - one_day).timestamp()

    # While loop through each month
    # while end < now:
    #     end = (datetime.fromtimestamp(start) + one_month).timestamp()
    try:
        for symbol in symbols[:10]:

            # Stock candles
            api_link = 'https://finnhub.io/api/v1/stock/candle?' \
                        + 'symbol={symbol}&resolution={resolution}&from={from_t}' \
                        + '&to={to_t}&token={token}'
            endpoint = api_link.format(symbol=symbol, resolution=resolution,
                from_t=str(int(start)), to_t=str(int(end)), token=api_key)
            response = http.get(endpoint)
            response_dict = json.loads(response.content.decode('utf-8'))

            if response_dict.get('s') == 'ok':
                # Convert to Pandas Dataframe and display datetime
                stock_candles = pd.DataFrame.from_dict(response_dict)
                stock_candles['s'] = 'ok'
                stock_candles['dt'] = [datetime.fromtimestamp(t) for t in stock_candles['t']]
                stock_candles.drop('t', axis=1, inplace=True)
                stock_candles['symbol'] = symbol
                    
                # Convert UTC to EST
                stock_candles['dt'] = stock_candles['dt'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

                # Export to csv
                buffer = StringIO()
                stock_candles.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # Copying the data from csv
                cur.copy_from(buffer, 'stock_candles_minute', sep=',')
                conn.commit()
            else:
                print('Sorry, no valid data for ' + symbol + ' at this moment')
            # start = end
        cur.close()
        send_msg('COMPLETED')
    except:
        send_msg('ALERT')
    return


# To get minute stock candles: resolution = 1
get_stock_min('1')

