from finnFetch import FinnFetch

import config

from utils.log_functions import print_log

import datetime as dt
import argparse

def args_resolve_timeback(timeback):
    n = timeback[0]
    unit = timeback[1]

    try:
        n = int(n)
    except:
        print(f"First -t arg must be int")
        return 1

    allowedUnits = ['d', 'w', 'm', 'y']
    if unit not in allowedUnits:
        print(f"Error: -t '{unit}' is invalid must be in {allowedUnits}")
        return 1
    timeUnitMap = dict(d=1, w=7, m=30, y=365)
    timeback = dict(days=n * timeUnitMap[unit])

    return timeback

def args_verify_timespan(timespan):
    start = int(timespan[0])
    end = int(timespan[1])

    if start > 0 and start < dt.datetime.today().timestamp() and end > start:
        return True
    print("Error: timespan")
    return False

def main(args):
    # Resolve arguments
    dry = args.dryrun
    symbol = args.symbol
    timeframe = args.resolution

    timespan = args.timespan
    if timespan and args_verify_timespan(timespan):
        start = timespan[0]
        end = timespan[1]
    else:
        timeback = args_resolve_timeback(args.timeback)
        end = dt.datetime.today() 
        start = end - dt.timedelta(**timeback)
        start = int(start.timestamp())
        end = int(end.timestamp())

    # Init client
    ff = FinnFetch()

    params = (symbol, timeframe, start, end)

    print_log("<<<<< BEGIN BATCH FETCH JOB <<<<<")
    attemptInsert = ff.fetchAndInsert_candle(params)
    if not attemptInsert['success']:
        msg = attemptInsert['msg']
        print(f"\n{msg}\n")

    if not dry:
        ff.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Contains functions to batch finnFetch up to 1 year data')
    parser.add_argument('symbol', type=str, help='Stock symbol')
    parser.add_argument('-r', '--resolution', '--timeframe', default='1', choices=config.available_timeframes)

    timeGroup = parser.add_mutually_exclusive_group()
    timeGroup.add_argument('-t', '--timeback', nargs=2, default=[1, 'y'], help='Units of time to go back | Ex: 1 d | Units allowed: y, m, d')
    timeGroup.add_argument('--timespan', nargs=2, type=int, help='Specify start and end timestamp')

    parser.add_argument('-x', '--dryrun', action='store_true') # For testing purposes
    args = parser.parse_args()
    main(args)
