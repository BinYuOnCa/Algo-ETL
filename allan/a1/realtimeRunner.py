from finnFetch import FinnFetch

import config

import utils.finnhub_functions as f_fun
import utils.twilio_sms as t_sms
from utils.log_functions import print_log

import datetime as dt
import re
import argparse

def convert_realtimeSymbols(rts, timeframes):
    toRun = dict([(i, []) for i in timeframes])

    for item in rts:
        for timeframe in item['resolutions']:
            timeframe = str(timeframe)
            if timeframe in toRun:
                toRun[timeframe].append(item['symbol'])

    return toRun

def main(timeframes):

    rtSymbols = config.settings_json['realtimeSymbols']

    ff = FinnFetch()
    
    print_log("<<<<< BEGIN FETCH JOB <<<<<")
    print(rtSymbols)
    # Run job for symbols
    toRun = convert_realtimeSymbols(rtSymbols, timeframes) 
    for timeframe, symbols in toRun.items():
        print(f"\nFetching '{timeframe}' data for the following symbols:\n")
        for symbol in symbols:
            print(symbol)
            startEnd = f_fun.getStartEnd(timeframe, dt.datetime(2021, 1, 11, 10, 1))
            params = (symbol, timeframe, startEnd['start'], startEnd['end'])

            attemptInsert = ff.fetchAndInsert_candle(params)
            try:
                if not attemptInsert['success']:
                    msg = attemptInsert['msg']
                    print_log(f">>>>> ERROR >>>>>\n{msg}")
                    # Notify by sms when error occurs
                    t_sms.sendMessage(f"ETL Job encountered error:\n{msg}")
                    ff.commit()     # Commit so previous and subsequent inserts are preserved
            except: # Unexpected Error occured
                print("\n||||| Unexpected Error occured |||||\n")
                
    ff.commit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run finnFetch to get realtime interval data')
    parser.add_argument('timeframes', action="extend", metavar='timeframes', nargs='+', choices=config.available_timeframes,
            help=f'runs script to fetch provided what timeframes  |  allowed values = {config.available_timeframes}')

    args = parser.parse_args()
    main(timeframes=args.timeframes)
