"""
Trading System Module

This part only holds the stack trading part.
Users can pass the order information of stocks, such as stack symbol,
price, and trading volume, as parameters into the TWS system to make
the entire trading process automatically.
"""

import time
import queue
import datetime
import pandas as pd
from datetime import date, datetime, timedelta
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.common import BarData
from ibapi.contract import Contract
from ibapi.order import *
from .tws_params import *


class IBapi(EWrapper, EClient):

    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.nextValidOrderId = 0
        self.account_buying_power_usd = TOTAL_BUY_POWER
        self.candles_data = []  # Initialize list to store candles
        self.time_queue = queue.Queue()  # Store time log
        self.errors_queue = queue.Queue()  # Store error message
        self.stock_df = pd.DataFrame(columns=["symbol", "trade_ID", "action", "quantity", "price"])
        self.reqIds(-1)  # Send request to server, ask for orderId. -1 default.

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # No connection build during the process
        self.disconnect()

    # ----- ----- -----
    # - ERROR HANDLE  -
    # ----- ----- -----

    def is_error(self):
        error_exist = not self.errors_queue.empty()
        return error_exist

    def get_error(self, timeout=5):
        if self.is_error():
            try:
                return self.errors_queue.get(timeout=timeout)
            except queue.Empty:
                return None
        return None

    # Override
    def error(self, reqId, errorCode, errorString):
        error_message = "Error returned at #{} with error code {} saying {}.".format(reqId, errorCode, errorString)
        self.errors_queue.put(error_message)

    # ----- ----- -----
    # - TIME RECORD   -
    # ----- ----- -----

    # Override
    def currentTime(self, server_time):
        self.time_queue.put(server_time)

    def server_clock(self):
        print("Asking server time...")
        self.reqCurrentTime()  # send time request

        try:
            server_time = self.time_queue.get(timeout=10)
        except queue.Empty:
            print("Cannot get the server time.")
            server_time = None

        while self.is_error():  # Check if error
            print(self.get_error())

        return server_time

    # ----- ----- -----
    # - OVERRIDE PART -
    # ----- ----- -----

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        print("Setting nextValidOrderId: {}".format(orderId))
        self.nextValidOrderId = orderId

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        super().updateAccountValue(key, val, currency, accountName)
        if key == BUYING_POWER_KEY and accountName == ACCOUNT:
            self.account_buying_power_usd = float(val)
            print("Buying power is {} in CAD.".format(self.account_buying_power_usd))

    def tickPrice(self, reqId, tickType, price, attrib):
        # print(tickType)
        if reqId == 1:
            td_date = date.today()
            yd_date = td_date - timedelta(days=1)
            if tickType == 68:
                print('The (delayed) last trade price is {} at {}'.format(
                    price, datetime.now() - timedelta(seconds=900)))
            elif tickType == 76:
                print("Today's ({}) open price: {}".format(td_date, price))
            elif tickType == 75:
                print("The prior day's ({}) closing price: {}".format(yd_date, price))
        else:
            print('Please check the reqId setting')

    def historicalData(self, reqId: int, bar: BarData):
        # print(bar)
        # print("Time:{} Close:{}".format(bar.date, bar.close))
        self.candles_data.append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume])

    def orderStatus(self, orderId, status, filled, remaining, avgFullPrice, permId, parentId, lastFillPrice, clientId,
                    whyHeld, mktCapPrice):
        print('orderStatus - orderid:', orderId, 'status:', status, 'filled', filled, 'remaining', remaining,
              'lastFillPrice', lastFillPrice)

    def openOrder(self, orderId, contract, order, orderState):
        print('openOrder id:', orderId, contract.symbol, contract.secType, '@', contract.exchange, ':', order.action,
              order.orderType, order.totalQuantity, orderState.status)

    def execDetails(self, reqId, contract, execution):
        print('Order Executed: ', reqId, contract.symbol, contract.secType, contract.currency, execution.execId,
              execution.orderId, execution.shares, execution.lastLiquidity)

    # ----- ----- ----- ---
    # - SELF DEFINED PART -
    # ----- ----- ----- ---

    @staticmethod
    def _build_contract(symbol):
        stock_contract = Contract()
        stock_contract.symbol = symbol
        stock_contract.secType = 'STK'
        stock_contract.exchange = 'SMART'
        stock_contract.currency = 'USD'
        return stock_contract

    def ask_stock_price(self, symbol):
        stock_contract = self._build_contract(symbol)
        # Request Market Price Data
        print("----- {} Price Info -----".format(symbol))
        self.reqMktData(1, stock_contract, '', False, False, [])

    def ask_hist_candles(self, symbol):
        stock_contract = self._build_contract(symbol)

        # Request historical candles
        print("----- {} Candles -----".format(stock_contract.symbol))
        self.reqHistoricalData(1, stock_contract, '', VWAP_WINDOW, VWAP_UNIT, 'TRADES', 0, 2, False, [])

    def calc_vwap(self):
        candles_df = pd.DataFrame(self.candles_data, columns=['timestamp', "open_price", "high_price", "low_price",
                                                              "close_price", "volume"])
        candles_df['timestamp'] = pd.to_datetime(candles_df['timestamp'])

        # Calculate vwap:
        def cal_avg(row):
            return (row["high_price"] + row["low_price"] + row["close_price"]) / 3

        candles_df["avg_price"] = candles_df.apply(cal_avg, axis=1)
        candles_df["cum_vol"] = candles_df["volume"].cumsum()
        candles_df["avg_tot_price"] = candles_df["volume"] * candles_df["avg_price"]
        candles_df["cum_tot_price"] = candles_df["avg_tot_price"].cumsum()
        candles_df["VWAP"] = candles_df["cum_tot_price"] / candles_df["cum_vol"]

        return candles_df

    def next_order_ID(self):
        order_id = self.nextValidOrderId
        self.nextValidOrderId += 1
        return order_id

    @staticmethod
    def create_order(price, quantity, action):
        trade_order = Order()
        trade_order.action = action  # BUY or SELL
        trade_order.totalQuantity = quantity
        trade_order.orderType = 'LMT'
        trade_order.lmtPrice = price
        return trade_order

    def place_order(self, symbol, action, price, quantity):
        # Place order:
        trade_order = self.create_order(price, quantity, action)
        trade_contract = self._build_contract(symbol)
        order_ID = self.next_order_ID()
        print("The valid order id of {} is: {}".format(symbol, order_ID))
        # execute order:
        print("{} {} of {} at {} is in processing.".format(action, symbol, quantity, price))
        self.placeOrder(order_ID, trade_contract, trade_order)
        stock_info = pd.DataFrame([symbol, order_ID, action, quantity, price],
                                  columns=["symbol", "trade_ID", "action", "quantity", "price"])
        self.stock_df = self.stock_df.append(stock_info)
        print("The order has been placed.")

    def check_order(self):
        print(self.stock_df)

    def cancel_order(self, order_ID):
        print("Cancelling order of {}".format(order_ID))
        print(self.stock_df[self.stock_df["trade_ID"] == order_ID])
        self.cancelOrder(order_ID)








