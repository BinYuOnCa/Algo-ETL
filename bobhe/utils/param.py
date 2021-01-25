import os


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
PATH_DATA = "./data/"
ONE_OFF_DAILY_BARS_PATH = PATH_DATA + 'one_off/'
ONE_OFF_MIN_BARS_PATH = PATH_DATA + 'one_off_min/'
DAILY_BARS_PATH = PATH_DATA + 'daily/'
MIN_BARS_PATH = PATH_DATA + 'min/'
ARCHIVE_PATH = PATH_DATA + 'archive/'
LOG_FILE_NAME = "./log/etl.log"
# retrieve finnhub api token from system environment
FINNHUB_API_TOKEN = os.environ.get('FINNHUB_API_TOKEN')
SP500_SYMBOL_URL = 'https://en.wikipedia.org/wiki/' + \
                   'List_of_S%26P_500_companies'
FINNHUN_REQUEST_URL = 'https://finnhub.io/api/v1/stock/candle?'
TABLE_NAME_DAILY_PRICES = 'daily_prices'
TABLE_NAME_MINS_PRICES = 'min_prices'
DAILY_BAR_DATA_ONE_OFF_START_DATE = '2002-02-01'
DAILY_BAR_DATA_ONE_OFF_END_DATE = '2021-01-23'
min_trade_date = ''  # YYYY-MM-DD
max_trade_date = ''  # YYYY-MM-DD
first_min_bar_trade_date = 0  # Unix time format
last_min_bar_trade_date = 0  # Unix time format
UNIX_ONE_DATE = 86400
INTERVAL_ONE_MINUTE = '1'
INTERVAL_ONE_DAY = 'D'
PROCESS_NORMAL = True
PROCESS_FAILURE = False
STOCK_DATA_AVAILABLE = True
STOCK_DATA_NOT_AVAILABLE = False
TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
FROM_PHONE_NO = os.environ.get('FROM_PHONE_NO')
TO_PHONE_NO = os.environ.get('TO_PHONE_NO')
FROM_EMAIL_ADDR = os.environ.get('FROM_EMAIL_ADDR')
TO_EMAIL_ADDR = os.environ.get('TO_EMAIL_ADDR')
GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.send']
LOG_LEVEL_INFO = 'info'
LOG_LEVEL_DEBUG = 'debug'
LOG_LEVEL_WARNING = 'warning'
LOG_LEVEL_ERROR = 'error'
LOG_LEVEL_CRITICAL = 'critical'
DEFAULT_LOG_LEVEL = 'INFO'
