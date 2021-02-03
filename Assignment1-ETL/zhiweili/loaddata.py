from lib.query import FinnhubQuery, OldDataQuery
from lib.persistent import SaveData
from datetime import datetime
from lib.messages import EmailNotification, SMSNotification
from finnhub import FinnhubAPIException, FinnhubRequestException
import sys
import csv
import pytz
import sched
import time
import logging
from lib import config


conf = config.get("APP")
logging.basicConfig(filename=conf['log_file'], level=logging.DEBUG)
est = pytz.timezone('US/Eastern')


def getCurrentTick(symbols, resolution, latest_tick, api: str = 'api'):
    finnhub = FinnhubQuery()
    now = datetime.now().astimezone(est).replace(tzinfo=None)
    now_seconds = int((now - datetime(1970, 1, 1)).total_seconds())
    result = []
    try:
        if(api == 'api'):
            result = finnhub.api_candles(symbols, resolution, latest_tick, now_seconds)
        else:
            result = finnhub.restful_candles(symbols, resolution, latest_tick, now_seconds)
    except FinnhubAPIException as error:
        logging.warn(error)
    except FinnhubRequestException as error:
        logging.warn(error)
    except Exception as error:
        logging.error(error)
    return result


def load_and_etl(symbol, resolution, api):
    db_tick_index = 6
    # db_open_index = 1
    tick_index = 5
    # open_index = 0

    latest_candle = OldDataQuery().latest_candle(symbol, resolution)
    if(latest_candle is not None and len(latest_candle) > 0):
        # Get latest tick time, compare it with retrieved tick and detect split/merge
        latest_tick = latest_candle[0][db_tick_index]
        logging.debug("loading " + symbol)
        candles_data = getCurrentTick(symbol, resolution, latest_tick, api)
        # same_tick_candle_data = list(filter(lambda x: (x[tick_index] == latest_tick), candles_data))
        # if(len(same_tick_candle_data) > 0):
        #     new_open = Decimal(same_tick_candle_data[0][open_index])
        #     old_open = latest_candle[0][db_open_index]
        #     if (abs(new_open - old_open) > 0.01):
        #         factor = new_open / old_open
        #         : update
        save_data = list(filter(lambda x: (x[tick_index] > latest_tick), candles_data))
        if(len(save_data) > 0):
            logging.debug("saving data for " + symbol)
            SaveData(resolution).candles(symbol, save_data)
        else:
            logging.debug("no new data")
    else:
        print("yes")
        start_tick = str(int((datetime(2021, 1, 22) - datetime(1970, 1, 1)).total_seconds()))
        candles_data = getCurrentTick(symbol, resolution, start_tick)
        print(candles_data)
        SaveData(resolution).candles(symbol, candles_data)


if __name__ == "__main__":
    args = config.getArgs(sys.argv[1:])
    resolution = args['resolution']
    api = 'api'
    if ('api' in args):
        api = args['api']

    s = sched.scheduler(time.time, time.sleep)
    logging.debug("Getting candle data in " + resolution)
    with open('sec_list_1000.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        delay = 0
        for row in spamreader:
            s.enter(delay, 1, load_and_etl, argument=(row[0], resolution, api))
            delay = delay + 1
            if(delay % 60 == 59):  # just in case
                delay = delay + 1
    s.run()

    msg = 'Finished loading daily level stock candles.'
    if(resolution == '1'):
        msg = 'Finished loading 1 minute level stock candles.'
    try:
        EmailNotification().send('zhiwilliam@gmail.com', msg)
        SMSNotification().send('+16475220400', msg)
    except Exception as error:
        logging.error('Failed to send message out')
        logging.error(error)
