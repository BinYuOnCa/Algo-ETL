import time
import ibapi
import ibapi.wrapper as wrapper
import ibapi.client as client
from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi import comm  # read message returned from API
from ibapi.account_summary_tags import AccountSummaryTags
from ibapi.utils import iswrapper


def createContract(
        symbol,
        sec_type='STK',
        exch='SMART',
        prim_exch='ISLAND',
        currency='USD'
):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.exchange = exch
    contract.primaryExch = prim_exch
    contract.currency = currency
    return contract


def createOrder(action, quantity, limit_price=None):
    def LimitOrder(action, quantity, limit_price):
        # ! [limitorder]
        order = Order()
        order.action = action
        order.orderType = "LMT"
        order.totalQuantity = quantity
        order.lmtPrice = limit_price
        # ! [limitorder]
        return order

    def MarketOrder(action, quantity):
        # ! [market]
        order = Order()
        order.action = action
        order.orderType = "MKT"
        order.totalQuantity = quantity
        # ! [market]
        return order

    if limit_price is None:
        return MarketOrder(action, quantity)
    else:
        return LimitOrder(action, quantity, limit_price)


class IBClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)


class IBWrapper(EWrapper):
    def __init__(self):
        EWrapper.__init__(self)


class IBClientApp(IBWrapper, IBClient):
    def __init__(self, IP, port, clientId):
        IBWrapper.__init__(self)
        IBClient.__init__(self, wrapper=self)
        self.IP = IP
        self.port = port
        self.clientId = clientId
        self.info = {'positions': []}
        self.history_data = {}
        self.order_record = {}
        self.permId2ord = {}

    def getConnection(self):
        self.connect(self.IP, self.port, self.clientId)
        self.reqIds(-1)
        self.getMessage(1)

    def getNextValidId(self):
        self.valid_id += 1
        return self.valid_id

    @iswrapper
    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.valid_id = orderId

    @iswrapper
    def error(self, reqId, errorCode, errorString):
        super().error(reqId, errorCode, errorString)
        if int(errorCode) >= 2000:
            return
        print('| Server return an error! reqId: %s, errorCode:%s, msg:%s' % (
            reqId, errorCode, errorString))

    def getAccInfo(self):
        self.info = {'positions': []}
        self.reqAccountSummary(102, "All", AccountSummaryTags.AllTags)
        self.reqPositions()
        time.sleep(2)
        self.cancelAccountSummary(102)
        self.getMessage(1)
        return self.info

    @iswrapper
    def accountSummary(self, reqId: int, account: str, tag: str, value: str,
                       currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        if (tag == 'TotalCashValue'):
            self.info['cash'] = value
        if (tag == 'NetLiquidation'):
            self.info['total'] = value

    @iswrapper
    def accountSummaryEnd(self, reqId: int):
        super().accountSummaryEnd(reqId)
        return

    @iswrapper
    def position(self, account, contract, position, avgCost):
        super().position(account, contract, position, avgCost)
        tmp = [contract.symbol, contract.secType, contract.currency, position,
               avgCost]
        self.info['positions'].append(tmp)

    @iswrapper
    def positionEnd(self):
        super().positionEnd()
        return

    def getHistoryData(self, reqId, symbol, queryTime, lastFor='10000 S',
                       timeGap='5 secs'):
        contract = createContract(symbol, 'STK', 'SMART', 'SMART', 'USD')
        self.reqHistoricalData(
            reqId, contract, queryTime,
            lastFor, timeGap, 'TRADES', 1, 1, []
        )

    @iswrapper
    def historicalData(self, reqId, date, open, high,
                       low, close, volume, barCount,
                       WAP, hasGaps):
        super().historicalData(reqId, date, open, high, low, close, volume,
                               barCount, WAP, hasGaps)
        if reqId not in self.history_data.keys():
            self.history_data[reqId] = []
        single_row = '%s,%s,%s,%s,%s,%s\n' % (
            date, open, high, low, close, volume
        )
        self.history_data[reqId].append(single_row)

    @iswrapper
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)

    def sendOrderToServer(
            self,
            symbol,
            quantity,
            sec_type='STK',
            primary_exch='SMART',
            price=None
    ):
        contract = createContract(
            symbol,
            sec_type,
            'SMART',
            primary_exch,
            'USD'
        )
        action = "BUY" if quantity > 0 else "SELL"
        order = createOrder(action, abs(quantity), price)
        orderId = self.getNextValidId()
        print('|- Place order. ID is %d' % orderId)
        self.placeOrder(orderId, contract, order)
        print(self.order_record)
        self.order_record[orderId] = [symbol, action, False]

    @iswrapper
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId,
                    parentId, lastFillPrice, clientId,
                    whyHeld, mktCapPrice):
        super().orderStatus(orderId, status, filled, remaining,
                            avgFillPrice, permId, parentId,
                            lastFillPrice, clientId, whyHeld, mktCapPrice)
        if status != 'Filled' or self.order_record[orderId][2]:
            return
        symbol = self.order_record[orderId][0]
        action = self.order_record[orderId][1]
        self.order_record[orderId][2] = True
        try:
            msg = '| %s Filled! %s quantity:%d avgPrice:%.2f Total:%.2f\n' % (
                time.strftime('%Y%m%d %H:%M:%S'),
                action, filled, avgFillPrice,
                filled * avgFillPrice
            )
            print(msg)
            self.logger.log(symbol, msg)
        except Exception:
            print('| Error in logger!')

    @iswrapper
    def openOrder(self, orderId, contract, order, orderState):
        super().openOrder(orderId, contract, order, orderState)
        # OpenOrder. ID: 2 UVXY STK @ SMART : BUY MKT 10.0 PreSubmitted
        print("OpenOrder. ID:", orderId, contract.symbol, contract.secType,
              "@", contract.exchange, ":", order.action, order.orderType,
              order.totalQuantity, orderState.status)
        order.contract = contract
        self.permId2ord[order.permId] = order

    @iswrapper
    def openOrderEnd(self):
        # ! [openorderend]
        super().openOrderEnd()
        print("OpenOrderEnd")
        # ! [openorderend]
        print("Received %d openOrders" % len(self.permId2ord))

    def getMessage(self, wait=3):
        time.sleep(wait)
        while not self.msg_queue.empty():
            text = self.msg_queue.get(block=True, timeout=0.2)
            fields = comm.read_fields(text)
            self.decoder.interpret(fields)


app = IBClientApp('127.0.0.1', 7497, clientId=0)
app.getConnection()
app.sendOrderToServer('IBM', 1)
app.disconnect()
