from datetime import datetime

import pytz

from utils.db import connect, get_candles_data
from utils.notice import send_sms, send_email
from utils.settings import DEV_DB_CONFIG, US_EQUITY_DAILY_FINN_TABLE, EMAIL_ACCOUNT, US_EQUITY_1M_FINN_TABLE
from utils.utiltools import get_logger

logger = get_logger()

param_dic = {
    "host": DEV_DB_CONFIG["HOST"],
    "database": DEV_DB_CONFIG["DATABASE"],
    "user": DEV_DB_CONFIG["USER"],
    "password": DEV_DB_CONFIG["PASSWORD"],
    "port": DEV_DB_CONFIG["PORT"]
}


def etl_to_db():
    try:
        file_list = open('./sec_list_1000.csv', 'r')
        symbol_list = file_list.readlines()
        conn = connect(param_dic, logger)
        for symbol in symbol_list:
            get_candles_data(conn, symbol.strip(), "D", US_EQUITY_DAILY_FINN_TABLE, logger)
            get_candles_data(conn, symbol.strip(), 1, US_EQUITY_1M_FINN_TABLE, logger)

        # logger.info("start remove_duplicate_data")
        # remove_duplicate_data(conn, US_EQUITY_1M_FINN_TABLE)
        # remove_duplicate_data(conn, US_EQUITY_DAILY_FINN_TABLE)

        conn.close()
        file_list.close()

        return True
    except Exception as e:
        logger.error(e)
        return False


if __name__ == '__main__':
    s = etl_to_db()
    if s:
        date = datetime.now().astimezone(pytz.timezone('US/Eastern')).replace(tzinfo=None).strftime('%Y-%m-%d')
        send_sms(f"Finish ETF for {date}")
        send_email(date, EMAIL_ACCOUNT)
        logger.info("send notice via sms and email")
