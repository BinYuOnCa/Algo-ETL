"""
Routine execute script to update the newest stack data to database.
"""
import yagmail
import pandas as pd
from tqdm import tqdm
from twilio.rest import Client
from datetime import datetime
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

import etl_utils.etl_main as etl
from etl_utils.etl_config import RDS_CONFIG, ALERT_CONFIG, USER_CUSTOM
from stack_info import STACK_LIST


def etl_main_process(table_name):
    """
    The main process to execute the ETL, download data from Finnhub and upload to RDS database.

    :param table_name: (str) Database default table name
    :return: None
    """

    with etl.connect_table(table_name) as db_table:
        if USER_CUSTOM["FIRST_RUN"]:
            stack_list = pd.DataFrame()
            stack_list.columns = ['symbol', 'last_time', 'volume']
        else:
            stack_list = db_table.stack_list()
        # Build process bar to estimate the routine executing time:
        t_stack_list = tqdm(STACK_LIST["name"])
        tb_name = db_table.tb_name
        for stack in t_stack_list:
            t_stack_list.set_description("{} | {}".format(tb_name, stack))
            etl.main_process(stack, db_table, stack_list)


def routine_process(alert=False, multi_process=False):
    """
    The routine process to update the newest stack data to database automatically.

    :param alert: (boolean) Send alert to phone and Email
    :param multi_process: (boolean) Set to parallel run daily and intraday check processes
    :return: None
    """
    start_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    # Set the Phone alert:
    account_sid = ALERT_CONFIG["ACCOUNT_SID"]
    auth_token = ALERT_CONFIG["AUTH_TOKEN"]
    client = Client(account_sid, auth_token)

    # Set the Email notification:
    yag = yagmail.SMTP(user=ALERT_CONFIG["EMAIL_SENDER_NAME"], password=ALERT_CONFIG["EMAIL_SENDER_PWD"])

    # Set the multi threads lock:
    total_lock = Lock()

    try:
        if multi_process:
            # Set the multiply threads:
            executor = ThreadPoolExecutor(max_workers=2)
            task_1 = executor.submit(etl_main_process, RDS_CONFIG["DAILY_TABLE"])
            task_2 = executor.submit(etl_main_process, RDS_CONFIG["INTRADAY_TABLE"])
            all_task = [task_1, task_2]
            for task in as_completed(all_task):
                print(task.result())
        else:
            etl_main_process(RDS_CONFIG["DAILY_TABLE"])
            etl_main_process(RDS_CONFIG["INTRADAY_TABLE"])
        end_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        email_msg = ("Routine task starts from {}, successful finished at {}.".format(start_time, end_time))
        if alert:
            yag.send(to=ALERT_CONFIG["EMAIL_RECEIVER"],
                     subject='ETL routine finished',
                     contents=email_msg)
        else:
            print(email_msg)

    except Exception as e:
        end_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        alert_info = "Because of {}, the {} was interrupted on {}.".format(e.__class__,
                                                                           RDS_CONFIG["INTRADAY_TABLE"],
                                                                           end_time)
        if alert:
            message = client.messages.create(from_=ALERT_CONFIG["TWILIO_PHONE"],
                                             to=ALERT_CONFIG["USER_PHONE"],
                                             body=alert_info)
            print(message.sid)
        raise Exception(alert_info)


if __name__ == '__main__':
    routine_process(alert=USER_CUSTOM["ALERT"], multi_process=USER_CUSTOM["MULTILINE"])
