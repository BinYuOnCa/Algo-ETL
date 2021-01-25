import os
import logging as log

# credential information
cred_info = {
    'api_key': os.environ['api_key'],
    'conn_str': os.environ['conn_str'],
    'email_addr': os.environ['email_addr'],
    'email_code': os.environ['email_code'],
    'sid': os.environ['sid'],
    'token': os.environ['token'],
    'free_tel': os.environ['free_tel'],
    'personal_tel': os.environ['personal_tel']
}

# column names of the tables in rds
column_names = {
    '1m': [
        'symbol', 'date_int_key', 'open_price', 'close_price', 'high_price',
        'low_price', 'volume', 'timestamp'
    ],
    '1d': [
        'symbol', 'date_int_key', 'open_price', 'close_price', 'high_price',
        'low_price', 'volume'
    ]
}

# csv file names to temporarily store the candles data
temp_csv = {'1m': './temp_1m.csv', '1d': './temp_daily.csv'}

# csv file names to store the symbols failed to be downloaded
failed_csv = {
    '1m': 'failed_symbols_1m.csv',
    '1d': 'failed_symbols_daily.csv'
}

# table names
tab_names = {'1m': 'us_equity_1m_finn', '1d': 'us_equity_daily_finn'}

# resolutions
resolution = {'1m': 1, '1d': 'D'}

# the limit of days that intraday candles can be downloaded at one time
intraday_limit = 30

# bytes to mega bytes
b_to_mb = 1024**2

# number of seconds in a day
day_to_sec = 24 * 60 * 60

# message template
msg_info = {
    'tmp': '{}\n\nHi Andy,\n\n{}\n\nBest regards,\nRobot',
    'comp': '''
    The ETL for {} is completed\n
    Time Period: {} to {}\n
    Time Spent: {}\n
    Records Added: {}\n
    ''',
    'fail': 'Failed to download {} symbols below:\n{}'
}

# logging
log.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                filename='./log')

# memory limit
mem_lim = 250

# unix time of 5 hrs
unix_5h = 60 * 60 * 5

