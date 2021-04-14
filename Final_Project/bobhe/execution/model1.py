from execution.params import *
from ibapi.contract import Contract
from utils.helpers import *
from ibapi.common import BarData
import collections
import pandas as pd
import time
import pause
import datetime
from execution.params import *
import pandas as pd

class Model1Stock:
    def __init__(self, contract, buying_thd):
        self.order_id = 0
        self.symbol = contract.symbol
        self.contract = contract
        self.machine_state = 'Observation'
        self.status = ''
        self.buy_opportunity = False
        self.unit_test_buy = "Buy"
        self.sell_price = 0.0
        self.sold_price = 0.0
        self.sold_time = None
        self.buy_vol = 0.0
        self.buy_price = 0.0
        self.bought_price = 0.0
        self.bought_time = None
        self.max_price_after_bought = 0.0
        self.stop_loss_price = 0.0
        self.trailing_stop_price = 0.0
        self.impatience = None
        self.hld_vol = 0.0
        self.rem_vol = 0.0
        self.buy_thd = buying_thd
        self.vwap_start = None
        self.vwap_updated = None
        self.data = collections.defaultdict(list)
        self.bar_1min = None
        self.vwap = 0.0
        self.ATR_ratio = 0.0
        self.last_tick_price = 0.0

    def state_machine_update(self):
        # making decision
        if self.machine_state == "Completion":
            return None
        elif self.machine_state == "Observation":
            if exceed_15h():
                self.machine_state = "Completion"
                return None
            else:
                if self.buy_opportunity:
                    # if uptrend patten is identified, when price pull back to vwap , trigger buy
                    # Case 1: new buy or
                    # Case 2: re-buy as previous buy order has been canceled: buy_price<(last_tick_price-0.5*ATR_ratio)
                    if self.buy_price < self.last_tick_price <= self.vwap:
                        # last tick price < the last vwap, buy condition is hit
                        # self.buy_price = self.vwap
                        self.buy_price = self.last_tick_price
                        self.machine_state = "Buying"
                        # trigger market buy order
                        return "BUY", self.buy_price
                else:
                    # check if buy opportunity is offered, e.g. price need to be in uptrend: higher high and higher low
                    self.check_buy_opportunity()
                    return None
        elif self.machine_state == "Buying":
            if self.status == 'Bought':
                self.machine_state = 'Hold'
                return None
            elif self.status == "Buying" and self.buy_price < (self.last_tick_price - 0.5 * self.ATR_ratio):
                # go back to observation state
                self.machine_state = 'Observation'
                # trigger cancel buy order, release fund for other usage . eg. buy other stock
                return "CBUY",0
            elif self.status == "Abandoned":
                # not enough funding
                self.machine_state = "Completion"
                return None
            else:
                return None
        elif self.machine_state == 'Hold':
            if self.stop_loss_price == 0.0:
                # only set the stop_loss_price once right after buy
                self.stop_loss_price = self.bought_price * (1 - LOSS_RATIO * self.ATR_ratio)
            # get the max price after bought
            self.max_price_after_bought = cal_max_price_after_bought(self.bar_1min, self.bought_time)
            # re-calculate the trailing_stop_price based on the latest max price after bought
            self.trailing_stop_price = self.max_price_after_bought * (1 - WIN_RATIO * self.ATR_ratio)
            if self.last_tick_price < self.stop_loss_price:
                # trigger MARKET SELL order
                self.machine_state = 'Stop_loss'
                return "SELL", 0
            if self.last_tick_price >= (self.bought_price + (1 + WIN_RATIO * self.ATR_ratio)):
                # A SELL STOP order is always placed below the current market price and
                # is typically used to limit a loss or protect a profit on a long stock position
                # refer to : https://interactivebrokers.github.io/tws-api/basic_orders.html
                self.machine_state = 'Profit_taking'
                return "SELL_STP", self.trailing_stop_price
            # else if the price is between the stop_loss_price and (bought_price + 6 ATR) , keep Hold
        elif self.machine_state == 'Profit_taking':
            # get the max price after bought
            self.max_price_after_bought = cal_max_price_after_bought(self.bar_1min, self.bought_time)
            # re-calculate the trailing_stop_price
            self.trailing_stop_price = self.max_price_after_bought * (1 - WIN_RATIO * self.ATR_ratio)
            if self.status == "Selling" and self.sell_price != self.trailing_stop_price:
                # only when price is still pushing higher, modify the sell STOP price
                # If price is going down, no need change on the sell STOP price
                return "MSELL_STP", self.trailing_stop_price
            if self.status == "Sold":
                self.machine_state = 'Completion'
                return None
        elif self.machine_state == 'Stop_loss' and self.status == "Sold":
            self.machine_state = 'Completion'
            return None
        else:
            return None

    def tick_price_update(self, price, tickType):
        if not DATA_SUBSCRIPTION:
            if self.last_tick_price != round(self.bar_1min['close'].iloc[-1], 3):
                self.last_tick_price = round(self.bar_1min['close'].iloc[-1], 3)
                return True
            return False
        if self.contract.symbol == 'FB' or 'AAPL' or 'JPM':
            if tickType == DELAYED_LAST:
                print(f"{datetime.datetime.now()} , security = {self.contract.symbol}, last price = {price}")
            if tickType == DELAYED_OPEN:
                print(f"{datetime.datetime.now()} , security = {self.contract.symbol}, opening price = {price}")
            if tickType == DELAYED_CLOSE:
                print(f"{datetime.datetime.now()} , security = {self.contract.symbol}, closing price = {price}")
        monitor_tp_list = [DELAYED_LAST, LAST]
        if tickType in monitor_tp_list and self.last_tick_price != price:
            self.last_tick_price = price
            return True
        return False

    def vwap_bar_should_refresh(self):
        if self.machine_state == "Completion":
            return False
        return True

    def update_bar_1min(self, bar: BarData):
        self.data['date'].append(bar.date[-8:])
        self.data['open'].append(bar.open)
        self.data['high'].append(bar.high)
        self.data['low'].append(bar.low)
        self.data['close'].append(bar.close)
        self.data['volume'].append(bar.volume)
        self.data['average'].append(bar.average)
        self.data['barCount'].append(bar.barCount)
        self.bar_1min = pd.DataFrame.from_dict(self.data)
        self.bar_1min.drop_duplicates(subset=['date'], keep='last')
        # self.bar_1min.set_index('date')

    def vwap_update(self, bar: BarData):
        print(f"vwap_update started")
        if DATA_SUBSCRIPTION:
            self.update_bar_1min(self, bar)
        self.bar_1min = calculate_vwap(self.bar_1min)
        if self.bar_1min is not None:
            if self.vwap != self.bar_1min['vwap'].iloc[-1]:
                self.vwap = self.bar_1min['vwap'].iloc[-1]
                self.vwap_updated = True
            else:
                self.vwap_updated = False
            self.bar_1min = calculate_atr(self.bar_1min, 14)
            self.ATR_ratio = self.bar_1min['ATR_ratio'].iloc[-1]
        return True

    def vwap_update_yf(self, df):
        print(f"vwap_update_yf started")
        self.bar_1min = calculate_vwap(df)
        if self.bar_1min is not None:
            if self.vwap != self.bar_1min['vwap'].iloc[-1]:
                self.vwap = self.bar_1min['vwap'].iloc[-1]
                self.vwap_updated = True
            else:
                self.vwap_updated = False
            self.bar_1min = calculate_atr(self.bar_1min, 14)
            self.ATR_ratio = self.bar_1min['ATR_ratio'].iloc[-1]
        return True

    def check_buy_opportunity(self):
        if self.vwap_updated:
            self.buy_opportunity = check_up_trend_patten_higher_high_higher_low(self.bar_1min, 30)


# for unit test only
if __name__ == "__main__":
    now = datetime.datetime.now()
    now2 = now.strftime("%H:%M:%S")
    print(f"now2 = {now2}")
    new_contract = Contract()
    new_contract.symbol = 'AAPL'
    new_contract.secType = 'STK'
    new_contract.exchange = 'SMART'
    new_contract.currency = 'USD'
    new_contract.primaryExchange = 'NASDAQ'

    my_model = Model1Stock(new_contract, 5000)
    bar = BarData()
    bar.date = "20210325  07:00:00"
    bar.open = 151.000000
    bar.high = 151.000000
    bar.low = 151.000000
    bar.close = 151.000000
    bar.volume = 2
    bar.average = 151.000000
    bar.barCount = 2
    my_model.update_bar_1min(bar)
    bar.date = "20210325  07:01:00"
    bar.open = 152.000000
    bar.high = 152.000000
    bar.low = 152.000000
    bar.close = 152.000000
    bar.volume = 2
    bar.average = 152.000000
    bar.barCount = 2
    my_model.update_bar_1min(bar)
    bar.date = "20210325  07:02:00"
    bar.open = 153.000000
    bar.high = 153.333333
    bar.low = 153.000000
    bar.close = 153.000000
    bar.volume = 2
    bar.average = 153.000000
    bar.barCount = 2
    my_model.update_bar_1min(bar)
    df2 = my_model.bar_1min
    df2.set_index('date')
    print(f"df2={df2}")
    print(df2.iloc[0]['date'])
    print(df2['high'].iloc[0:3].max())
    print(cal_max_price_after_bought(df2,now))
