from utils.db_util import get_first_trade_date
from utils.db_util import get_last_trade_date
from utils.db_util import get_first_min_bar_trade_date
from utils.db_util import get_last_min_bar_trade_date
from utils.db_util import download_daily_bars
from utils.db_util import download_intraday_bars
from utils.db_util import upload_one_off_bars_file
from utils.convert_date_time_format import convert_unix_date_format
from utils.convert_date_time_format import get_today_date
from utils.sms import send_twilio_message
from utils.mail import send_gmail_message
import utils.param as param
from utils.log import init_log
from utils.log import output_log


def require_one_off_historical_daily_data():
    """
    check ETL for one off historical daily bar data if requires
    :param:
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    # Initialize one_off_require as False
    one_off_require = False
    # get first and last trade date in DB daily_prices table
    if get_first_trade_date() and get_last_trade_date():
        if param.min_trade_date != param.DAILY_BAR_DATA_ONE_OFF_START_DATE:
            # if minimum trade data not equal to
            # the predefine one off start date
            one_off_require = True
    else:
        # The table daily_prices is empty
        output_log('daily_prices table is empty')
        one_off_require = True
    return one_off_require


def require_one_off_historical_mins_data():
    """
    check ETL for one off historical minutes bar data if requires
    :param:
    :return one_off_require: (bool) True or False
    """
    one_off_require = False
    if get_first_min_bar_trade_date() and get_last_min_bar_trade_date():
        if param.first_min_bar_trade_date == 0 \
                or param.last_min_bar_trade_date == 0:
            one_off_require = True
    else:
        output_log('mins_prices table is empty')
        one_off_require = True
    return one_off_require


def download_one_off_daily_bars():
    """
    downlaoding the one off daily bars data from finnhub
    :param:
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_NORMAL
    one_off_start_date = convert_unix_date_format(
        param.DAILY_BAR_DATA_ONE_OFF_START_DATE)
    one_off_end_date = convert_unix_date_format(
        param.DAILY_BAR_DATA_ONE_OFF_END_DATE)
    output_log(f'one_off_start_date: {one_off_start_date}')
    output_log(f'one_off_end_date: {one_off_end_date}')
    status = download_daily_bars(one_off_start_date, one_off_end_date)
    return status


def process_etl_one_off_daily_bars():
    """
    processing the one off daily bars data
    :param:
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_NORMAL
    if download_one_off_daily_bars() and \
        upload_one_off_bars_file(param.ONE_OFF_DAILY_BARS_PATH,
                                 param.TABLE_NAME_DAILY_PRICES):
        return status
    else:
        status = param.PROCESS_FAILURE
        return status


def download_one_off_intraday_bars(interval='1'):
    """
    Downloading the one off intraday bars data
    :param interval: str
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_NORMAL
    one_off_end_date = convert_unix_date_format(get_today_date())
    one_off_start_date = one_off_end_date - param.UNIX_ONE_DATE * 365
    output_log(f'one_off_start_date: {one_off_start_date}')
    output_log(f'one_off_end_date: {one_off_end_date}')
    status = download_intraday_bars(interval,
                                    one_off_start_date,
                                    one_off_end_date)
    return status


def process_etl_one_off_intraday_bars():
    """
    procesing the ETL one off intraday bars
    :param N/A
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_NORMAL
    if download_one_off_intraday_bars(interval=param.INTERVAL_ONE_MINUTE) \
            and upload_one_off_bars_file(param.ONE_OFF_MIN_BARS_PATH,
                                         param.TABLE_NAME_MINS_PRICES):
        return status
    else:
        status = param.PROCESS_FAILURE
        return status


if __name__ == "__main__":
    """
    ETL one_off historical data
    """
    init_log()
    txt1 = 'ETL one-off historical load'
    global status
    status = process_etl_one_off_daily_bars()
    status = process_etl_one_off_intraday_bars()
    if status == param.PROCESS_NORMAL:
        txt2 = f'{txt1} finished successfully'
        output_log(f'{txt2}')
        send_twilio_message(f'{txt2}')
        send_gmail_message(f'{txt2}', 'Amazing!')
    else:
        txt2 = f'{txt1} failed'
        output_log(f'{txt2}', param.LOG_LEVEL_CRITICAL)
        send_twilio_message(f'{txt2}')
        send_gmail_message(f'{txt2}', 'Check and fix it!')
