
from ibapi.contract import Contract



class MondelGene:
    def __init__(self, contract, buy_thd, open_below):
        self.symbol = contract.symbol
        self.contract = contract
        self.open_limit = open_below
        self.active = "BUY"  # "BUY"  or "SELL"
        self.order_type = "LMT"

        # 运行时的变量runtime variables
        self.order_id = None
        self.buy_price = None
        self.sell_price = None
        self.open_price = None
        self.nbb = None
        self.nba = None
        # self.nbbg = np.inf
        self.last_trade_price = None

        # 倒数计时？Count Down
        self.impatience = None

        # VMAP相关变量
        self.bar_1min = dict()
        # self.bar_1min_parttime = np.array([])
        self.vwap = None
        self.vwap_second = None  # this is the pending vwap
        self.vwap_moving = None  # moving vwap is not used now
        self.vwap_updated = False  # 当vwapy准备使用时
        self.vwap_start = None  # when did vwap update starts / restarts

        # 最后的变量
        self.bought_price = None
        self.bought_time = None
        self.sold_price = None
        self.sold_time = None
        self.reached_purchase = False

        # 状态变量
        self.status = None  # {None->buying->bought->selling->sold->cancelling}
        self.buy_vol = 0
        self.bld_vol = 0
        self.rem_vol = 1  # 想修改的订单，订单里面的股票交易数量






if __name__ == "__main__":
    MondelGene("ABC", 1, 1)

