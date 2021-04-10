param_dic = {
    "host": "192.168.1.18",  # aws:35.155.65.198
    "database": "postgres",
    "user": "postgres",
    "password": "12345678"
}


ACCOUNT = "DU3310615"
# 单笔最高投资额。老师是设计为可以同时持有12只股票，所以这里就设置为6万/12=5千
INVESTMENT_AMT = 1000   # 单笔交易我自己定最高限额买1000美元
# 你账户里面最多放的钱，这里举例是6万美元
TOTAL_BUY_POWER = 60000

PRY_XCH = {
    "SPCE": "NYSE",
    "LX": "NASDAQ"
}

DEBUG = True  # 这里用不同的对应不同参数
if DEBUG:
    ACCOUNT = ""  # paper trading
    IBIP = '127.0.0.1'
    PORT = 7497
else:
    ACCOUNT = ""  # prod trading
    IBIP = '127.0.0.1'
    PORT = 7496

STP_BUY_HR = 15   # 下午三点后不会交易

MAX_TRACING = 100   # 同时交易最多100个股票

MIN_PRICE = 2

MAX_TRANS = 21

MAX_ORDER_FREQ = 5
MIN_ODR_CLR_SP = 3600

# 最小止损点
MAX_STP = -0.001  # M52
MIN_STP = -0.08  # M52 亏损达到8%就一定卖出止损
# 最大终盈点
MAX_PFT = 0.08  # M52
MIN_PFT = 0.01  # M52

VWAP_LEEWAY_COE = 0.002
VWAP_LEEWAY_COE_RT = 0.003
# 分钟数据可能会丢失，存在missing data，
# 如果分数表，一天内的350分钟的数据少于250个，那么放弃这个股票，因为没有足够能力去算法
# 这个参数用在回测中
MIN_1MIN_QUALITY = 250

SELL_VWAP_BLO = 0.01
BUY_BLO = 0.00
# 五秒钟如果还没成交，且价格也变了，那么就取消。如果五秒钟到了我要买的价格还是属于最优价格，那么可以再等
PATIENCE_LIMIT_BUY = 5  # for M55, in seconds. The time before cancelling an unsuitable order  卖股票用
PATIENCE_LIMIT_SELL = 60   # 60秒，这个不取消，因为可以一直等待

SIMU_BUY_PERFECTION_ERROR = 0.0
# 考虑到买单有时间延迟，所以我们在贵1分钱的时候我们就发单
PROD_BUY_EARLIER_MIN = 0.01  # 离我们价格只差1分钱的差价，用于普通股票
PROD_BUY_EARLIER_PERC = 0.001   #  0.1%的差价，用于贵的股票

TIME_ZONE_ADJ = 0   # 温哥华写-3
