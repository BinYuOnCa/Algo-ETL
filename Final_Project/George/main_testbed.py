import time
import threading
from execution.robot_testbed import IBapiTest
from utils.params import *
import pandas as pd


app = None


def run_loop():
    app.run()


def execution_main():
    global app

    app = IBapiTest()

    app.connect(IBIP, PORT, 555)

    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()
    app.reqAccountUpdates(True, ACCOUNT)

    time.sleep(10)  # give some time for the system to fill the orders


    orderid = app.nextValidOrderId
    print(f"now next orderid is {orderid}")

    app.reqMarketDataType(3) #Delayed
    app.get_stock_price("TSLA")


    time.sleep(1)

    ticker_to_buy = pd.read_csv("logs/ticker_to_buy.csv")
    ticker_to_sell = pd.read_csv("logs/ticker_to_sell.csv")

    if len(ticker_to_buy) > 0:
        for index, row in ticker_to_buy.iterrows():
            ticker = row.ticker
            quantity = 100 # need to update quantity based on buying power and stock pricing after fixing getting the trading value
            app.algo_order(ticker, "BUY", quantity) #


    app.nextValidId(orderid)
    print(f"after method, now next orderid is {app.nextValidOrderId}")

    time.sleep(10)

    app.disconnect()


if __name__ == "__main__":
    execution_main()