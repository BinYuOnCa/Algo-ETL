import pandas as pd
import time
from datetime import date, datetime
import configparser
import logging
from logging.handlers import TimedRotatingFileHandler
import utils.insert_tb as insert_tb
import utils.stock as stock
import utils.message as message

# logging init
fmt_str = '%(asctime)s[level-%(levelname)s][%(name)s]:%(message)s'
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter(fmt_str)
logfile = './dy-candl.log'  # os.path.abspath(os.curdir)
handler = TimedRotatingFileHandler(logfile,
                                   when="D",
                                   interval=1,
                                   backupCount=10)
handler.setFormatter(formatter)
handler.suffix = "%Y%m%d_%H%M%S.log"
logger.addHandler(handler)
logtxt = " ETL Starts"
emailtxt = str(datetime.now()) + logtxt + '\n'
logger.info(logtxt)

# get parameters
config = configparser.ConfigParser()
config.read('./settings.ini')
api_key = config['finnhub']['api_key']
resolution = config['finnhub']['intervals']
# steps = int(config['finnhub']['steps'])  # how many resolutions
symbol_file = config['finnhub']['symbol_file']

now = int(time.time())
start_time = None  # now-86400
today = date.today().strftime('%Y-%m-%d')

# get symbol list
logtxt = " read symbols"
logger.info(logtxt)
symbols = pd.read_csv(symbol_file, header=None, names=['symbol_name'])

# get split infor of today
split_df = stock.split_df(symbols=symbols, api_key=api_key, end_date=today)
logtxt = " split obtained records = " + str(split_df.shape[0])
emailtxt += str(datetime.now()) + logtxt + '\n'
logger.info(logtxt)

# upsert split to the db
if config['split']['write_db'] == '1' and split_df.shape[0] > 0:
    insert_tb.refresh(split_df, 'split')
    logtxt = " upserted split to the db = "
    emailtxt += str(datetime.now()) + logtxt + '\n'
    logger.info(logtxt)

# delete splited symbol from related tables
if config['split']['reload_cand'] == '1' and split_df.shape[0] > 0:
    insert_tb.del_by_symbol(split_df['symbol'], 'us_equity_daily_finn')
    insert_tb.del_by_symbol(split_df['symbol'], 'us_equity_1_finn')
    logtxt = " deleted splited symbols from tables"
    logger.info(logtxt)

# get day candles
stock.candles(
    symbols=symbols,
    resolution='D',
    api_key=api_key,
    start_time=start_time,
    end_time=now)
logtxt = " daily candles finished"
emailtxt += str(datetime.now()) + logtxt + '\n'
logger.info(logtxt)

# get minutes candles
stock.candles(
    symbols=symbols,
    resolution='1',
    api_key=api_key,
    start_time=start_time,
    end_time=now)
logtxt = " minutes candles finished"
emailtxt += str(datetime.now()) + logtxt + '\n'
logger.info(logtxt)

# send email alert
if config['email']['yes'] == '1':
    message.send_email(email_txt=emailtxt, attach=logfile, subject=None)
    logger.info(' email sent to ' + config['email']['receiver'])

# send sms alert
if config['sms']['yes'] == '1':
    sid = message.send_sms(emailtxt)
    logger.info(' sms sent with sid = ' + config['sms']['to'])

logtxt = " ETL ends"
emailtxt = str(datetime.now()) + logtxt + '\n'
logger.info(logtxt)
