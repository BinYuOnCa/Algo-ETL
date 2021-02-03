from etl_one_off import process_etl_one_off_daily_bars
from etl_one_off import process_etl_one_off_intraday_bars
from etl_one_off import require_one_off_historical_daily_data
from etl_one_off import require_one_off_historical_mins_data
from etl_daily import process_etl_incremental_daily_bars
from etl_daily import process_etl_incremental_intraday_bars
from utils.sms import send_twilio_message
from utils.mail import send_gmail_message
from utils.log import init_log
from utils.log import output_log
import utils.param as param


if __name__ == "__main__":
    """
    ETL both one_off and incremental data load, triggered by con job
    """
    init_log()
    txt = '!!!!!!!!!!!!!!!!!!!!!!!!!!!ETL started!!!!!!!!!!!!!!!!!!!!!!!!!!!'
    output_log(f'{txt}')
    exit(0)
    send_twilio_message(f'{txt}')
    send_gmail_message(f'{txt}', 'Be patient!')
    global status
    if require_one_off_historical_daily_data():
        status = process_etl_one_off_daily_bars()
    else:
        status = process_etl_incremental_daily_bars()
    if require_one_off_historical_mins_data():
        status = process_etl_one_off_intraday_bars()
    else:
        status = process_etl_incremental_intraday_bars()
    if status == param.PROCESS_FAILURE:
        txt = 'ETL failed'
        output_log(f'{txt}', param.LOG_LEVEL_CRITICAL)
        send_twilio_message(f'{txt}')
        send_gmail_message(f'{txt}', 'Check and fix it!')
    else:
        txt = 'ETL finished successfully'
        output_log(f'{txt}')
        send_twilio_message(f'{txt}')
        send_gmail_message(f'{txt}', 'Bravo! Good night!')
