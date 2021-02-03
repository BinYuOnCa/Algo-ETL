from utils.db_util import get_first_trade_date
from utils.db_util import get_last_trade_date
from utils.db_util import get_first_min_bar_trade_date
from utils.db_util import get_last_min_bar_trade_date
from utils.db_util import download_intraday_bars
from utils.db_util import upload_incremental_bars_file
from utils.db_util import download_daily_bars
from utils.convert_date_time_format import convert_unix_date_format
from utils.convert_date_time_format import get_today_date
from utils.sms import send_twilio_message
from utils.mail import send_gmail_message
from archive import archive_data
import utils.param as param
from utils.log import init_log
from utils.log import output_log


def download_incremental_daily_bars():
    """
    downloading the incremental daily bars data from finnhub
    :param:
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    output_log('download_incremental_daily_bars')
    status = param.PROCESS_NORMAL
    incremental_start_date = convert_unix_date_format(
        param.max_trade_date)
    # minus one unix day to ensure the last trade day data will be retrieved
    incremental_start_date = incremental_start_date - param.UNIX_ONE_DATE
    incremental_end_date = convert_unix_date_format(get_today_date())
    output_log(f'incremental start_date: {incremental_start_date}')
    output_log(f'incremental end_date: {incremental_end_date}')
    status = download_daily_bars(incremental_start_date, incremental_end_date)
    return status


def download_incremental_intraday_bars(interval='1'):
    """
    downloading the incremental intraday bars data from finnhub
    :param interval: str
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    output_log('download_incremental_intraday_bars')
    status = param.PROCESS_NORMAL
    if param.first_min_bar_trade_date == 0 or \
            param.last_min_bar_trade_date == 0:
        status = param.PROCESS_FAILURE
        output_log('first and last trade day for min bar is empty',
                   param.LOG_LEVEL_WARNING)
        return status
    else:
        min_bar_data_start_date = param.last_min_bar_trade_date - \
                              param.UNIX_ONE_DATE
    output_log(f'min_bar_data_start_date: {min_bar_data_start_date}')
    min_bar_data_end_date = convert_unix_date_format(get_today_date())
    output_log(f'min_bar_data_end_date: {min_bar_data_end_date}')
    status = download_intraday_bars(param.INTERVAL_ONE_MINUTE,
                                    min_bar_data_start_date,
                                    min_bar_data_end_date)
    return status


def process_etl_incremental_daily_bars():
    """
    process the ETL incremental daily bars data
    :param:
    :return Bool: PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_NORMAL
    if get_first_trade_date() and get_last_trade_date():
        output_log('process_etl_incremental_daily_bars started')
        if download_incremental_daily_bars() and \
            upload_incremental_bars_file(param.DAILY_BARS_PATH,
                                         param.TABLE_NAME_DAILY_PRICES):
            archive_data(param.DAILY_BARS_PATH, param.ARCHIVE_PATH)
            output_log('process_etl_incremental_daily_bars successful')
        else:
            status = param.PROCESS_FAILURE
            output_log('process_etl_incremental_daily_bars failed',
                       param.LOG_LEVEL_WARNING)
    else:
        status = param.PROCESS_FAILURE
    return status


def process_etl_incremental_intraday_bars():
    """
    process the ETL incremental intraday bars data
    :param:
    :return status: (bool) PROCESS_NORMAL or PROCESS_FAILURE
    """
    status = param.PROCESS_NORMAL
    if get_first_min_bar_trade_date() and get_last_min_bar_trade_date():
        output_log('process_etl_incremental_intraday_bars started')
        if download_incremental_intraday_bars(
            interval=param.INTERVAL_ONE_MINUTE) \
            and upload_incremental_bars_file(param.MIN_BARS_PATH,
                                             param.TABLE_NAME_MINS_PRICES):
            archive_data(param.MIN_BARS_PATH, param.ARCHIVE_PATH)
            output_log('process_etl_incremental_intraday_bars successful')
        else:
            status = param.PROCESS_FAILURE
            output_log('process_etl_incremental_intraday_bars failed',
                       param.LOG_LEVEL_WARNING)
    else:
        status = param.PROCESS_FAILURE
    return status


if __name__ == "__main__":
    """
    ETL daily incremental data
    """
    init_log()
    txt1 = 'ETL daily incremental load'
    global status
    status = process_etl_incremental_daily_bars()
    status = process_etl_incremental_intraday_bars()
    if status == param.PROCESS_NORMAL:
        txt2 = f'{txt1} finished successfully'
        output_log(f'{txt2}')
        send_twilio_message(f'{txt2}')
        send_gmail_message(f'{txt2}', 'Awesome')
    else:
        txt2 = f'{txt1} failed'
        output_log(f'{txt2}', param.LOG_LEVEL_CRITICAL)
        send_twilio_message(f'{txt2}')
        send_gmail_message(f'{txt2}', 'Check and fix it!')
