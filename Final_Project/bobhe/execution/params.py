import os

DEBUG = True

DATA_SUBSCRIPTION = False

INVESTMENT_AMT = 5000

TOTAL_BUY_POWER = 60000

PRY_XCH = {
    "SPCE": "NYSE",
    "LX": "NASDAQ"
}

if DEBUG:
    ACCOUNT = "bobgxp123"  # paper trading
    IBIP = '127.0.0.1'
    PORT = 7497
else:
    ACCOUNT = ""  # prod trading
    IBIP = ''
    PORT = 7496


BUYING_POWER_KEY = 'BuyingPower'
STP_BUY_HR = 15

MAX_TRACING = 100

MIN_PRICE = 2

MAX_TRANS = 21

#MAX_ORDER_FREQ = 5
MAX_ORDER_FREQ = 30
MIN_ODR_CLR_SP = 3600  # 1h

MAX_STP = -0.001  # M52
MIN_STP = -0.08  # M52

MAX_PFT = 0.08  # M52
MIN_PFT = 0.01  # M52

VWAP_LEEWAY_COE = 0.002
VWAP_LEEWAY_COE_RT = 0.003

MIN_1MIN_QUALITY = 250

SELL_VWAP_BLO = 0.01
BUY_BLO = 0.00

PATIENCE_LIMIT_BUY = 5  # for M55, in seconds. The time before cancelling an unsuitable order
PATIENCE_LIMIT_SELL = 60

SIMU_BUY_PERFECTION_ERROR = 0.0

PROD_BUY_EARLIER_MIN = 0.01
PROD_BUY_EARLIER_PERC = 0.001

TIME_ZONE_ADJ = 0

SECURITIES_LIST_FILE = './secs_list.csv'
SECURITIES_NATR_FILE = './secs_buy_thd111.csv'

DELAYED_BID = 66
DELAYED_ASK = 67
DELAYED_LAST = 68
DELAYED_BID_SIZE = 69
DELAYED_ASK_SIZE = 70
DELAYED_LAST_SIZE = 71
DELAYED_HIGH = 72
DELAYED_LOW = 73
DELAYED_VOLUME = 74
DELAYED_CLOSE = 75
DELAYED_OPEN = 76
OPEN = 14
BID_SIZE = 0
BID = 1
ASK = 2
ASK_SIZE = 3
LAST = 4
LAST_SIZE = 5
HIGH = 6
LOW = 7
VOLUME = 8
CLOSE = 9
RT_VOLUME = 48

ODD_LOT_CRT_LMT = 2.0
IB_TRADE_COST = 0.005
window_size = 30
WIN_RATIO = 2.5
LOSS_RATIO = 1

# Local DB
# DB_HOST = 'localHOST'
# DB_NAME = 'algotrade'
# RDS
DB_HOST = 'algotradeDB.cni1a98czedc.us-east-2.rds.amazonaws.com'
DB_NAME = 'algotrade1'
# retrieve DB USER NAME from system environment
DB_USER = os.environ.get('DB_USER_NAME')
# retrieve DB PASSWORD from system environment
DB_PASSWORD = os.environ.get('DB_PASSWORD')
SP500_SYMBOL_URL = 'https://en.wikipedia.org/wiki/' + \
                   'List_of_S%26P_500_companies'



