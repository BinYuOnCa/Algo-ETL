"""
This script is to test the automatic trading system using IBapi on TWS.
(2021-04-06)
"""

import time
import threading
from datetime import datetime
from tws_utils.trade_sys import IBapi
from .tws_params import *


def run_loop():  # Keep the connection
    test_app.run()


def execution_main():
    global test_app

    test_app = IBapi()
    test_app.connect(host=IBIP, port=PORT, clientId=1)

    # Build socket connection:
    tws_thread = threading.Thread(target=run_loop, daemon=True)
    tws_thread.start()

    # Check the account details:
    test_app.reqAccountUpdates(True, ACCOUNT)

    # Get the stock info:
    test_app.reqMarketDataType(3)  # live (1) frozen (2) delayed (3) delayed frozen (4).

    # Ask for APPLE's price:
    test_app.ask_stock_price("AAPL")
    time.sleep(5)

    # Check server time:
    server_time = test_app.server_clock()
    server_time = datetime.fromtimestamp(server_time)
    print("The current server time is {}".format(server_time))

    # Check orders:
    test_app.reqAllOpenOrders()
    
    # Ask for APPLE's past 10 hours candles:
    test_app.ask_hist_candles("AAPL")
    time.sleep(5)

    # Print out the calculated vwap:
    candles_df = test_app.calc_vwap()
    print(candles_df)

    # Send a buying order (test):
    # test_app.place_order('AAPL', 'BUY', '50.00', 100)
    # time.sleep(10)

    # Check the placed orders:
    test_app.check_order()
    # Then we can modify or cancel orders based on the record.

    # Cancel the order:
    # test_app.cancel_order()

    time.sleep(5)
    test_app.disconnect()


if __name__ == "__main__":
    execution_main()
