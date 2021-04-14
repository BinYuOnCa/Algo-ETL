# from execution.robot_textbed import IBapiTest
from ibapi.order import Order
from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract
import time
import datetime
from ibapi import comm  # read message returned from API
from ibapi.account_summary_tags import AccountSummaryTags
# from execution.robot_textbed import IBapiTest
from ibapi.order import Order
import threading
import psycopg2
import sys
from utils import stock_pgfunctions as pg
from sec_instance import MondelGene  # 各种变化参数
from params import *  # 各种固定参数
from utils import stock_other_functions as oth

class IBClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)


class IBWrapper(EWrapper):
    def __init__(self):
        EWrapper.__init__(self)


class IBClientApp(IBWrapper, IBClient):
    def __init__(self, sec_instance):
        IBWrapper.__init__(self)
        IBClient.__init__(self, wrapper=self)
        self.sec_instance = sec_instance
        self.valid_id = 0  # 仅仅为了定义下，因为真正的订单编号是靠BI自带的orderId获得
        self.order_record = {}
        self.nextValidOrderId = 0
        self.aa = {}
        self.order_set = dict()
        self.buy_vol = 1
        self.sql = ""
        self.last_price = 0
        orderId = self.valid_id

    @staticmethod
    def myconnect():
        """
        Connect to the PostgreSQL database server.
        :return: conn or None
        """
        try:
            conn = psycopg2.connect(**param_dic)
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"error:{error},param_dic:{param_dic}")
            sys.exit(1)
            return None
        return conn

    def get_connection(self):
        """
        亲测可用
        和TWS连接，务必记得TWS要实现配置好
        """
        # self.connect("127.0.0.1", 7497, clientId=0)
        self.connect(IBIP, PORT, clientId=555)
        # 这里设定-1是一个假象，是第一次执行（前提是你确保你是第一次或TWS已经重置了，那么你就这里设置当前第一次可用为-1，等着后面+1
        # 这是BI自己的函数 void reqIds	(int numIds)
        self.reqIds(-1)
        self.get_message()

    def get_connection_Master_API(self):
        """
        官网说必须在TWS上的Master API Client ID设置为非负数才能使用到Master Client ID
        "To receive commissions reports for all clients it is necessary to connect as the Master Client ID"
        然后才能使用execDetails?
        """
        self.eConnect(IBIP, PORT, clientId=555)
        self.reqIds(-1)
        self.get_message()


    def get_message(self):
        """
        亲测可用
        用来抓信息的
        """
        time.sleep(2)
        while not self.msg_queue.empty():
            text = self.msg_queue.get(block=True, timeout=0.2)
            # print("貌似下面会接着自动调用nextValidId函数")
            # print(f"报文:{text}")
            fields = comm.read_fields(text)
            self.decoder.interpret(fields)

    def modify_order(self, order_id, modify_price: float):
        """
        亲测可行
        改单子。其实就是把之前的下单动作重新一模一样做了一遍，注意订单编号也要和之前一样哦
        """
        # print("开始执行改单子")
        order = Order()
        order.action = self.sec_instance.active
        order.totalQuantity = self.sec_instance.rem_vol
        order.account = ACCOUNT   # 这个参数在params.py 文件中
        order.orderType = self.sec_instance.order_type   # 改单子一般都是用限价而不是市场价
        order.lmtPrice = modify_price    # 外部设置价格
        # contract = self.create_contract(symbol, 'STK', 'SMART', 'SMART', 'USD')
        self.placeOrder(order_id, self.sec_instance.contract, order)
        # print(f"这单子ID: {order_id}股票{self.sec_instance.symbol}价格被修改为{order.lmtPrice}"
        #       f" for {order.totalQuantity}")
        self.sql = f"INSERT INTO stock_orders(order_id, symbol, price, active,remark,dt) " \
            f"VALUES({order_id},'{self.sec_instance.symbol}',{modify_price},'{self.sec_instance.active}','修改订单','{datetime.datetime.today()}')"
        pg.execute_sql_not_return(self.sql)

    def cancel_remaining_buy(self, order_id):
        """
        亲测可行
        取消下单行为，其实就是之前发过的单号，赋值给self.cancelOrder这个BI自己的函数就完事了。垃圾，这里都不会给任何反馈信息啊
        """
        print("准备取消下单行为")
        # 由于交易毫秒之间，在你执行这个取消交易动作之前或许已经有一部分成交了，只有一部分还没成交，所以这里做一次确认。
        cancel_id = order_id
        self.cancelOrder(cancel_id)  # 发出取消通知，这是系统自己的函数

    # 如果你不disconnect一直在执行的话，那么这个累加就有效果，否则没啥用
    # def get_next_valid_id(self):
    #     self.valid_id += 1
    #     return self.valid_id

    def nextValidId(self, orderId: int):
        print(f"在nextValidId里得到下一个有效订单号码: { orderId}")
        # 得到例如在nextValidId里得到下一个有效订单号码: 68 亲测可用
        """
        亲测可用
        很神奇的是，这个函数没有经过任何调用，就能在连接的时候自动执行。这就是老师说的接口。类似JAVA的接口。
        我们这里的my_order_id,是一个仅仅在这个函数内有用的局部变量。仅仅是为了满足下nextValidId的调用后就自然获得订单号码。神奇。
        my_order_id 在这里，从头到尾，就没变化过，所以说明是只要执行了nextValidId就会自然获得这个订单编号了
        """
        super().nextValidId(orderId)   # 看起来似乎没啥用，据说是为了安全起见，重写被调用的函数先把原函数执行一下
        self.nextValidOrderId = orderId  # 给内部变量赋上返回的ID

    def determine_buy_volume(self, price):
        """
        做配平,看下该买多少股。例如本来计算机是要98股，那么你就配平到100股。这是为了让券商帮你更好成交
        """
        ODD_LOT_CRT_LMT = 100  # 不知道哪里取，临时写死
        if price < ODD_LOT_CRT_LMT:
            self.buy_vol = round((INVESTMENT_AMT // price) / 100.0) * 100
        else:
            self.buy_vol = INVESTMENT_AMT // price
        return self.buy_vol

    def my_send_order(self):
        """
        亲测可用
        下订单的程序，可买卖
        """
        print("开始执行下单函数my_send_order")
        self.valid_id = self.nextValidOrderId
        print('|- Place order. ID 数值是: %d' % self.valid_id)
        # 这里Order()开始下订单了，是重点
        order = Order()
        order.action = self.sec_instance.active  # BUY 或者 SELL
        order.account = ACCOUNT
        order.orderType = self.sec_instance.order_type     # MKT市场价格  LMT限价
        if order.orderType == "LMT":
            order.lmtPrice = self.last_price  # 自己试一试加价买
            order.totalQuantity = self.buy_vol
        else:
            order.totalQuantity = 1   # 不知道怎么拿到市场价格，只好写死
        self.placeOrder(self.valid_id, self.sec_instance.contract, order)
        print(f"订单执行完毕了，你能在客户端软件上看到订单了")
        if order.orderType == "LMT":
            self.sql = f"INSERT INTO stock_orders(order_id, symbol, price, active,remark,dt) " \
                f"VALUES({self.valid_id},'{self.sec_instance.symbol}',{order.lmtPrice},"\
                f"'{self.sec_instance.active}','LMT','{datetime.datetime.today()}')"
        else:
            self.sql = f"INSERT INTO stock_orders(order_id, symbol, price, active,remark,dt) " \
                       f"VALUES({self.valid_id},'{self.sec_instance.symbol}',null," \
                       f"'{self.sec_instance.active}','MkT','{datetime.datetime.today()}')"
        pg.execute_sql_not_return(self.sql)

    def placeOrder(self, a, b, c):
        """
        亲测可用, 被动函数
        这里重写它，其实就是为了在未来能找到我现在发出的订单情况，便于日后回忆用.
        override  发单的函数，在EClient里面：contract是这支股票object，不是只是股票代码.
        这个函数很重要.
        """
        super().placeOrder(a, b, c)  # 创建一个实例
        self.aa = {"ins": a, "order是": b}
        # self.nextValidOrderId += 1
        print(f"BI的placeOrder收到的执行指令是:{self.aa}")  # 可惜没看到价格啊??
        # 得到{'ins': 68, 'order': 2539970209536: 0,IBM,STK,,0.0,,,SMART,,USD,,,False,,combo:}

    def updateAccountValue(self, a, b, c, d):
        """
        亲测可用。这是被动调用的函数, 收各种垃圾信息。
        坑，看了官方的python源代码wrapper.py的第190行，说这个函数得在有人为调用了reqAccountUpdates(True, ACCOUNT) 才会被触发
        所以，这个是无法单独执行的。
        这个乱七八糟信息很多，官话叫账户组合信息
        """
        super().updateAccountValue(a, b, c, d)
        # print(f"文本：{a}分隔{b}分隔{c}分隔{d}")
        if a == "NetLiquidationByCurrency":
            print(f"账户还剩下这点钱了：{b}")
        if a == "StockMarketValue":
            print(f"股票市场价值：{b}")
        if a == "TotalCashBalance":
            print(f"现金价值：{b}")
        if a == "FullAvailableFunds":
            print(f"完全可用的Funds:{b}")
        if a == "FullMaintMarginReq":
            print(f"完全保证金要求:{b}")
        if a == "FullInitMarginReq":
            print(f"完全初始证金要求:{b}")
        if a == "FullExcessLiquidity":
            print(f"看不懂的FullExcessLiquidity:{b}")
        if a == "GrossPositionValue":
            print(f"总头寸价值:{b}")
        if a == "EquityWithLoanValue":
            print(f"有贷款价值的东东:{b}")

    def ret_api_orders(self):  # 垃圾，没效果啊，取消不掉啊
        print("把服务器当前所有的订单取消掉")
        self.reqOpenOrders()

    def tickPrice(self, reqId, tickType, price, attrib):
        """
        亲测可用
        override 只要有价格更新就会立刻调用这个函数。非常频繁调用的函数。一秒钟可能4百次。
        tickType是tick_price.py里面的各种代号。
        任何一个交易的价格，都会通过这个函数发过来。这个函数就是起到收集市场最新价格作用。
        但是只能监听100只股票。
        最高更新频率是每个股票价格监听更新是一秒四次，100只股票就是每秒更新400次。
        这个函数得在有人为调用了以下三个才能显示数据：
        reqMarketDataType(3)
        app.reqMktData(55, contract, '', False, False, [])
        app.get_message()
        必须三个都显示才会被触发
        即你得告诉BI你想知道的合同号码，和get_message()显示
        :param reqId: int 随便写，我都是乱写一个数字，主要是靠app.reqMktData的contract参数，对应你要找的股票
        :param tickType: int 价格代号， 通过我oth.tickType_meaning可查询对应含义，比如o,c,h,l, last价格等
        :param price: float 具体价格数字
        :param attrib:  搞不懂为啥都是给我反馈0
        :return:  没有return，都是回调
        """
        super().tickPrice(reqId, tickType, price, attrib)
        # 提前两分钟。9：30开市，我们9：28就开始接受1秒钟四次的数据啦
        # 因为IB证券商在开盘的时候会发一个非常重要的数据（代号14--opening price，这个数据每只股票只发一次），且卡着9：30发给你
        # 所以我们务必在9:30之前就让机器人做好准备。因为券商发这个开市价格可能会几毫秒提前或延迟
        tickType_mean = oth.tickType_meaning(tickType)
        # print(f"=={reqId}=={tickType}={tickType_mean}:{price}=={attrib}")
        # 例如得到了 ==1==DELAYED_LAST:68==136.12==CanAutoExecute: 0, PastLimit: 0, PreOpen: 0
        if self.sec_instance.contract.secType == "STK":
            if tickType_mean == "DELAYED_LAST":
                self.last_price = price  # 这下总算是有最新价格数据了，虽然因为我用免费的是15分钟延迟，付费才能实时
                print(f"得到延迟15分钟的最新价格是{self.last_price}")
        if self.sec_instance.contract.secType == "CASH":
            if tickType_mean == "ASK":
                self.last_price = price  # 这下总算是有最新价格数据了，虽然因为我用免费的是15分钟延迟，付费才能实时

    def show_last_price(self):
        """
        Set  data type 听说要设置marketdata且给股票才能得到tickPrice
        https://interactivebrokers.github.io/tws-api/market_data_type.html
        1是实时数据，要收费订阅才行。3是延迟数据，免费。
        配合tickPrice才有用
        """
        self.reqMarketDataType(3)  # 订阅市场数据，可以执行没报错
        self.reqMktData(3, self.sec_instance.contract, '', False, False, [])  # 可以执行没报错
        self.get_message()
        return self.last_price

    # override  收信息。这是被动调用的函数，没测试过
    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        super().updateAccountValue(key, val, currency, accountName)
        print(f"buying power updated:{key} == {val}=== {currency}=={accountName}")

    # override 这个也是被动调的函数。没测试过
    def orderStatus(self, orderId: int, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining,
                            avgFillPrice, permId, parentId, lastFillPrice,
                            clientId, whyHeld, mktCapPrice)
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
              "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
              "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
              lastFillPrice, "ClientId:", clientId, "WhyHeld:",
              whyHeld, "MktCapPrice:", mktCapPrice)

    # override  没测试过。听说这个是可以看到返回的订单状态（判断你是否最后成交），为啥我啥也没看到？
    def execDetails(self, reqId, contract, execution):
        super().execDetails(reqId, contract, execution)
        print("ExecDetails=", execution.orderId, contract.symbol, contract.secType, contract.currency,
              execution.shares, execution.side, execution.avgPrice)


# 一些全局参数
app = None
price = 10


def create_contract(mysymbol, sec_type, exch, prim_exch, currency):
    """
    亲测可用
    全局使用。按照标准的自带的Contract()格式制作，没啥可调的，按规矩做就是了。
    """
    contract = Contract()  # 自己的
    contract.symbol = mysymbol
    contract.secType = sec_type
    contract.exchange = exch
    contract.primaryExch = prim_exch
    contract.currency = currency
    return contract


def run_loop():
    app.run()


def execution_main():
    global app
    # symbol_list = pg.find_valid_symbol_bymodel()
    # if len(symbol_list) == 0:
    #     print("没有可交易的股票")
    # else:
    #     # 怎么获取这个股票的当前最新价格呢？
    #     print(symbol_list)  # ['AAP', 'ABG', 'ABMD', 'ACN', 'ADBE']

    symbol = "IBM"
    contract = create_contract(symbol, 'STK', 'SMART', 'SMART', 'USD')  # 合同，具体股票代码

    # symbol = "EUR"
    # contract = create_contract(symbol, 'CASH', 'IDEALPRO', 'SMART', 'USD')  # 合同，具体股票代码

    # # 多线程Start the socket in a thread
    # api_thread = threading.Thread(target=run_loop, daemon=True)
    # api_thread.start()

    # app.modify_order(sec_instance, price)


    # 把contract扔进去，让机器人跑跑看合适不合适， 后面1，1没意义，纯瞎设的
    sec_instance = MondelGene(contract, 1, 1)   # 创建一个含有买卖合同的实例对象
    app = IBClientApp(sec_instance)
    app.get_connection()
    # app.get_connection_Master_API   # ERROR -1 504 Not connected
    my_last_price = app.show_last_price()

    # print("可以执行。用来updateAccountValue知道账户还剩下这点钱。但是必须配合get_message()使用")
    # app.reqAccountUpdates(True, ACCOUNT)
    # app.get_message()


    print("开始执行reqHistoricalData，执行了啥也不显示，垃圾")
    app.reqHistoricalData(75,
                           contract,
                           "", "1 D", "1 min", "TRADES", 1, 1, True,
                           [])
    app.get_message()






    # print("可能会显示出updateAccountValue")
    # print("测试！！！")
    # order = Order()  # 得到0,0,0 和一串乱码，看不懂
    # print(order)

    # 9:30 AM to 4:00 PM ET交易时间内才可以
    app.my_send_order()


    # print("可以执行。开始修改订单")
    # sql = "select order_id from stock_orders where dt='2021-04-05 15:08:10.654917' "
    # my_edit_id = (pg.execute_sql(sql))[0][0]
    # print(my_edit_id)
    # app.modify_order(my_edit_id, my_last_price-5)

    # print("可以执行。开始取消订单")
    # sql = "select order_id from stock_orders where dt='2021-04-05 15:08:10.654917' "
    # my_edit_id = (pg.execute_sql(sql))[0][0]
    # app.cancel_remaining_buy(my_edit_id)


    # app.cancel_remaining_buy()

    print("每次一到断开连接disconnect的时候就会报错OSError: "
          "[WinError 10038] An operation was attempted on something that is not a socket，坑")
    time.sleep(2)
    app.disconnect()


if __name__ == "__main__":
    execution_main()




