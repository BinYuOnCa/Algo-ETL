import math
import datetime
import numpy as np
from .params import *
from collections import deque
from ibapi.order import Order
from ibapi.common import BarData
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from utils.helper import market_opened


class IBapi(EWrapper, EClient):
    def __init__(self, watch_set, buying_power):
        EClient.__init__(self, self)
        self.nextValidOrderId = 0
        self.buying_power = buying_power
        self.account_buying_power_usd = buying_power  # the actual account's buying power, use when simultaneous buy.
        self.watch_set = watch_set  # {stream_id: sec_instance}
        self.order_set = dict()  # {order_id: {'ins':sec_instance, 'order': order}}
        self.bar_set = dict()  # {bar_id: sec_instance}
        self.buying_orders = dict()  # {order_id: time}
        self.dq_bought_time = deque(maxlen=MAX_ORDER_FREQ)  # buy frequency limit
        self.reqIds(-1)

    # override
    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        super().updateAccountValue(key, val, currency, accountName)
        if key == BUYING_POWER_KEY and accountName == ACCOUNT:
            self.account_buying_power_usd = float(val) / CURRENCY_CVT[currency]
            print(f"buying power updated: {self.account_buying_power_usd}, at {datetime.datetime.now()}")

    # override
    def placeOrder(self, reqId, sec_instance, order):
        sec_instance.order_id = reqId
        super().placeOrder(reqId, sec_instance.contract, order)
        self.order_set[reqId] = {"ins": sec_instance, "order": order}
        self.nextValidOrderId += 1

    # override
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        print(f"Setting nextValidOrderId: {orderId}")
        self.nextValidOrderId = orderId

    def modify_sell_order(self, sec_instance, price):
        order = Order()
        order.action = "SELL"
        order.totalQuantity = sec_instance.rem_vol  # current holding volume
        order.account = ACCOUNT
        order.orderType = "LMT"
        order.lmtPrice = price
        self.placeOrder(sec_instance.order_id, sec_instance, order)
        print(f"ID: {sec_instance.order_id} -- Modifying Selling {sec_instance.contract.symbol} at {order.lmtPrice}"
              f" for {order.totalQuantity}")

    def send_sell_order(self, sec_instance, price):
        order = Order()
        order.action = "SELL"
        order.totalQuantity = sec_instance.rem_vol  # current holding volume
        order.account = ACCOUNT
        order.orderType = "LMT"
        order.lmtPrice = price
        self.placeOrder(self.nextValidOrderId, sec_instance, order)
        print(f"ID: {sec_instance.order_id} -- Selling {sec_instance.contract.symbol} at {order.lmtPrice}"
              f" for {order.totalQuantity}")

    def determine_buy_volume(self, price):
        if price < ODD_LOT_CRT_LMT:
            buy_vol = round((INVESTMENT_AMT // price) / 100.0) * 100  # m57
        else:
            buy_vol = INVESTMENT_AMT // price
        return buy_vol

    def send_buy_order(self, sec_instance, price):
        buy_vol = self.determine_buy_volume(price)
        cost = buy_vol * price + buy_vol * IB_TRADE_COST  # m57
        if self.buying_power < cost:
            print(f"Total spent limit reached, can't buy {sec_instance.contract.symbol} at {price}.")
            return False
        elif self.account_buying_power_usd < cost:
            print(f"Account's buying power is insufficient for buying {sec_instance.contract.symbol} at {price}.")
            return False
        else:
            self.buying_power -= cost
            sec_instance.buy_vol = buy_vol
            order = Order()
            order.action = "BUY"
            order.totalQuantity = buy_vol  # take the floor
            order.account = ACCOUNT
            order.orderType = "LMT"
            order.lmtPrice = price
            self.placeOrder(self.nextValidOrderId, sec_instance, order)
            self.buying_orders[sec_instance.order_id] = datetime.datetime.now()
            print(f"ID: {sec_instance.order_id} -- Buying {sec_instance.contract.symbol} at {order.lmtPrice}"
                  f" for {order.totalQuantity}. Buying power available: {self.buying_power}")
            return True

    def execute_stploss(self, sec_instance):
        order = Order()
        order.action = "SELL"
        order.totalQuantity = sec_instance.rem_vol  # current holding volume
        order.orderType = "MKT"
        order.account = ACCOUNT
        self.placeOrder(self.nextValidOrderId, sec_instance, order)
        print(f"ID: {sec_instance.order_id} -- Stopping loss {sec_instance.contract.symbol} for"
              f" {order.totalQuantity}")

    def cancel_remaining_buy(self, sec_instance):
        if sec_instance.status == "Bought" and sec_instance.hld_vol < sec_instance.buy_vol:
            self.cancelOrder(sec_instance.order_id)
            return True
        return False

    def _bought_queue_not_full(self):
        now = datetime.datetime.now()
        while len(self.dq_bought_time) > 0 and (now - self.dq_bought_time[0]["time"]).total_seconds() > MIN_ODR_CLR_SP:
            self.dq_bought_time.popleft()
        if len(self.dq_bought_time) < MAX_ORDER_FREQ:
            return True
        else:
            return False

    def execute_action(self, action, sec_instance):
        direction, price = action
        if price == 0 and direction == "SELL":
            self.execute_stploss(sec_instance)
            sec_instance.status = "Selling"
            sec_instance.sell_price = None
        elif direction == "SELL":
            self.send_sell_order(sec_instance, price)
            sec_instance.sell_price = price
            sec_instance.status = "Selling"
        elif direction == "BUY":
            if self._bought_queue_not_full():
                if self.send_buy_order(sec_instance, price):
                    sec_instance.status = "Buying"
                else:
                    sec_instance.status = "Abandoned"
                    print(self.dq_bought_time)
            else:
                print(f"Buying {sec_instance.contract.symbol} failed due to full bought queue: ")
                print(self.dq_bought_time)
                sec_instance.status = "Abandoned"
        elif direction == "CBUY" or direction == "CSELL":
            if sec_instance.order_id is not None:
                sec_instance.status = "Cancelling"
                self.cancelOrder(sec_instance.order_id)
                print(f"Cancelling order {sec_instance.order_id} for security {sec_instance.symbol}")
                if direction == "CBUY" and sec_instance.order_id in self.buying_orders:
                    del self.buying_orders[sec_instance.order_id]
            else:
                print(f"no order found for {sec_instance.symbol} action {direction}")
        elif direction == "MSELL":
            sec_instance.sell_price = price
            self.modify_sell_order(sec_instance, price)
        return False

    def ret_api_orders(self):
        self.reqOpenOrders()

    # override
    def tickPrice(self, reqId, tickType, price, attrib):
        super().tickPrice(reqId, tickType, price, attrib)
        if market_opened(TIME_ZONE_ADJ, min_bfr=2):
            if self.watch_set[reqId].tick_price_update(price, tickType):
                action = self.watch_set[reqId].state_machine_update()
                if action is not None:
                    return self.execute_action(action, self.watch_set[reqId])
        return None

    def clear_position(self):
        print("Clearing positions!")
        self.reqGlobalCancel()
        for sec_instance_key in self.watch_set:
            if self.watch_set[sec_instance_key].rem_vol != 0:
                self.execute_stploss(self.watch_set[sec_instance_key])
        return True

    # override
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice,
                    permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice,
                            clientId,whyHeld, mktCapPrice)
        sec_instance = self.order_set[orderId]["ins"]
        order = self.order_set[orderId]["order"]
        if status == "Cancelled":
            if order.action == "BUY":
                self.buying_power += sec_instance.buy_vol * sec_instance.buy_price
                if sec_instance.hld_vol == 0:
                    sec_instance.status = None
                    sec_instance.nbbg = np.inf
                    self.buying_power += sec_instance.buy_vol * IB_TRADE_COST
                else:
                    self.buying_power -= filled * avgFillPrice
                    self.buying_power -= filled * IB_TRADE_COST
                print(f"Order Cancelled {sec_instance.order_id} for security {sec_instance.symbol}, bp: {self.buying_power}")
            else:
                sec_instance.status = "Bought"
        if order.action == "BUY":
            print("OrderStatus ID: ", orderId, ", Status ", status, ", Filled ", filled, ", Remaining ",
                  remaining, ", AvgFillPrice ", avgFillPrice)
        else:
            print(f"OrderStatus ID:  {orderId} {sec_instance.contract.symbol} bought at {sec_instance.bought_price}"
                  f" sold at {sec_instance.sold_price} has remaining {sec_instance.rem_vol} of total"
                  f"{sec_instance.hld_vol}")

    def cancel_all_buying(self):
        for order_id in self.buying_orders:
            self.cancelOrder(order_id)
        self.buying_orders.clear()

    def refresh_vwap_bars(self):
        for req_id in self.bar_set:
            sec_instance = self.bar_set[req_id]
            if sec_instance.vwap_bar_should_refresh():
                cancelled = self._cancel_bar_data(bar_id=req_id)
                self._remove_from_dict(cancelled, self.bar_set)
                self._request_vwap_bar(sec_instance)
                print(f"refreshing vwap bar for {sec_instance.symbol} ... ")

    def _remove_from_dict(self, keys, d):
        if keys is not None and len(keys) > 0:
            for each_key in keys:
                del d[each_key]

    def _request_vwap_bar(self, sec_instance):
        for bar_id in self.bar_set:
            if self.bar_set[bar_id].symbol == sec_instance.symbol:
                print(f"Avoid requesting multiple vwap bars for security {sec_instance.symbol}")
                return False
        self.reqHistoricalData(self.nextValidOrderId,
                               sec_instance.contract,
                               "", "1 D", "1 min", "TRADES", 1, 1, True,
                               [])
        sec_instance.vwap_start = datetime.datetime.now()
        self.bar_set[self.nextValidOrderId] = sec_instance
        self.nextValidOrderId += 1
        return True

    def _handle_buy_order_fill(self, execution):
        now = datetime.datetime.now()
        sec_instance = self.order_set[execution.orderId]["ins"]
        sec_instance.hld_vol += execution.shares
        sec_instance.rem_vol += execution.shares
        sec_instance.bought_time = now
        sec_instance.bought_price = execution.avgPrice
        sec_instance.status = "Bought"
        sec_instance.impatience = None
        sec_instance.vwap_start = now
        self._request_vwap_bar(sec_instance)
        for each in self.dq_bought_time:
            if each["order_id"] == sec_instance.order_id:
                each["time"] = now
                break
        else:
            self.dq_bought_time.append({"time": now, "order_id": sec_instance.order_id})
            if sec_instance.order_id in self.buying_orders:
                del self.buying_orders[sec_instance.order_id]
            if not self._bought_queue_not_full():
                self.cancel_all_buying()

    def _handle_sell_order_fill(self, execution):
        now = datetime.datetime.now()
        sec_instance = self.order_set[execution.orderId]["ins"]
        sec_instance.rem_vol -= execution.shares
        sec_instance.sold_time = now
        sec_instance.sold_price = execution.avgPrice
        sec_instance.status = "Sold"
        sec_instance.impatience = None
        self.buying_power += float(execution.avgPrice) * float(execution.shares)
        cancelled = self._cancel_bar_data(sec_instance=sec_instance)
        self._remove_from_dict(cancelled, self.bar_set)
        self.cancel_remaining_buy(sec_instance)
        print(f"Buying power available: {self.buying_power}")

    # override
    def execDetails(self, reqId, contract, execution):
        super().execDetails(reqId, contract, execution)
        if execution.side == "BOT":
            self._handle_buy_order_fill(execution)
        elif execution.side == "SLD":
            self._handle_sell_order_fill(execution)
        print("ExecDetails. ", execution.orderId, contract.symbol, contract.secType, contract.currency,
              execution.shares, execution.side, execution.avgPrice)

    def _cancel_bar_data(self, stream_id=None, bar_id=None, sec_instance=None):
        if bar_id is not None:
            self.cancelHistoricalData(bar_id)
            if bar_id in self.bar_set:
                print(f"Cancelled bar {bar_id} for security {self.bar_set[bar_id].symbol}")
                return [bar_id]
            else:
                print(f"Keep cancelling bar {bar_id} without having it in records")
        else:
            if stream_id is not None:
                sec_instance = self.watch_set[stream_id]
            if sec_instance is not None:
                to_be_deleted = list()
                for bar_id in self.bar_set:
                    if self.bar_set[bar_id].symbol == sec_instance.symbol:
                        self.cancelHistoricalData(bar_id)
                        print(f"Cancelled bar {bar_id} for security {sec_instance.symbol}")
                        to_be_deleted.append(bar_id)
                if len(to_be_deleted) == 0:
                    print(f"No cancellable bar found for security {sec_instance.symbol}")
                else:
                    return to_be_deleted
            else:
                print(f"No cancellable bar without input")

    # override
    def openOrder(self, orderId, contract, order, orderState):
        super().openOrder(orderId, contract, orderId, orderState)
        if order.action == "BUY" and datetime.datetime.now().hour >= STP_BUY_HR:
            self.cancelOrder(orderId)
            print(f"Cancelling Openorder. ID: {orderId}, {contract.symbol}, {contract.secType} @ {contract.exchange}, :"
                  f"{order.action}, {order.orderType}, {order.totalQuantity}, {orderState.status}")
        else:
            print(f"Openorder. ID: {orderId}, {contract.symbol}, {contract.secType} @ {contract.exchange}, :"
                  f"{order.action}, {order.orderType}, {order.totalQuantity}, {orderState.status}")

    def historicalData(self, reqId: int, bar: BarData):
        if reqId in self.bar_set:
            sec_instance = self.bar_set[reqId]
            if market_opened(TIME_ZONE_ADJ):
                sec_instance.vwap_update(bar)
        else:
            print(f"historical data request {reqId} is cancelled, but still getting data {bar}")
            self._cancel_bar_data(bar_id=reqId)
        return None

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        if reqId in self.bar_set:
            super().historicalDataEnd(reqId, start, end)
            sec_instance = self.bar_set[reqId]
            sec_instance.vwap_updated = True
            print("HistoricalDataEnd. for ", sec_instance.symbol, "ReqId:", reqId, "from", start, "to", end)
        else:
            print(f"historical data request {reqId} is cancelled, but still getting data")
            self._cancel_bar_data(bar_id=reqId)

    def historicalDataUpdate(self, reqId: int, bar: BarData):
        if reqId in self.bar_set:
            sec_instance = self.bar_set[reqId]
            if market_opened(TIME_ZONE_ADJ):
                if sec_instance.vwap_update(bar):
                    action = sec_instance.state_machine_update()
                    if action is not None:
                        return self.execute_action(action, sec_instance=sec_instance)
        else:
            print(f"historical data request {reqId} is cancelled, but still getting data {bar}")
            self._cancel_bar_data(bar_id=reqId)
        return None
