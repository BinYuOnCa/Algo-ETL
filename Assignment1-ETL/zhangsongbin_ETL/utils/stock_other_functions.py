from twilio.rest import Client
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
import smtplib
import time
import datetime
import logging
import traceback
from utils.stock_settings import Settings


"""
There are the functions about log, msg and time.
Log.
Send message to mobile device.
Send message to e-mailbox.
Convert to timestamp.
Calculate the timestamp of N days before now.
"""


stock_settings = Settings()
param_dic = stock_settings.param_dic
email_server = stock_settings.email_server
email_nm = stock_settings.email_name
email_from_addr = stock_settings.email_from_addr
email_password = stock_settings.email_password
email_to_addr = stock_settings.email_to_addr
sms_id = stock_settings.sms_id
sms_token = stock_settings.sms_token
sms_from = stock_settings.sms_from
sms_to = stock_settings.sms_to


def log(error, remark=""):
    """
    Logging to file
    :param error:
    :param remark:
    :return: 1
    """
    logging.basicConfig(
        filename='log_record.txt',
        filemode='a',  # a表示附加，如果改为w就是覆盖
        format='%(asctime)s, %(name)s - %(levelname)s - %(message)s',
        level=40    # Level Num 40 ERROR
    )
    # notice, It is a wrong grammar:  logging.DEBUG
    logging.error(f"!!!!!=={remark}=================Your program has an error===============")
    logging.error(error)
    logging.error(traceback.format_exc())
    # logging.shutdown()  # close file
    return 1


def send_email(table, email_title):
    """
    Send mail
    :param table:
    :param email_title:
    :return:
    """
    msg = MIMEText(f"""
    Hi, {email_nm}：
    This is a notice email. Your {table} downloading and importing to PostregSQL has completed.
    """, 'plain', 'utf-8')
    s = 'Your EC2 <%s>' % email_from_addr
    email_name, email_addr = parseaddr(s)
    msg['From'] = formataddr((Header(email_name, 'utf-8').encode(), email_addr))
    s = 'Give you <%s>' % email_to_addr
    email_name, email_addr = parseaddr(s)
    msg['To'] = formataddr((Header(email_name, 'utf-8').encode(), email_addr))
    msg['Subject'] = Header(email_title, 'utf-8').encode()
    server = smtplib.SMTP_SSL(email_server, 465)
    server.set_debuglevel(0)  # if you set (1), you will get detail.
    try:
        server.login(email_from_addr, email_password)
    except smtplib.SMTPAuthenticationError as error:
        print("Send email error.AuthenticationError.")
        return 1
    except Exception as error:
        print("Send email error.")
        return 1
    try:
        server.sendmail(email_from_addr, [email_to_addr], msg.as_string())
        print("Send email completed.")
    except smtplib.SMTPRecipientsRefused as error:
        print("Send email error.Refused.")
        return 1
    except Exception as error:
        print("Send email error.")
        return 1
    server.quit()


def send_sms(msg):
    """
    Send sms.
    :param msg:
    :return: 1
    """
    account_sid = sms_id
    auth_token = sms_token
    client = Client(account_sid, auth_token)
    message = client.messages.create(
                         body=msg,
                         from_=sms_from,
                         to=sms_to
                     )
    print(message.sid)
    return 1


def to_time_stamp(df):
    """
    Input data string, convert to timestamp.
    :param df:str
    :return: timestamp
    """
    # 转换成时间数组
    time_array = time.strptime(df, "%Y-%m-%d %H:%M:%S")
    # 转换成时间戳
    timestamp = time.mktime(time_array)
    return timestamp


def get_day_time(n):
    """
    Calculate the timestamp of N days before now
    :param n: int
    :return: timestamp
    """
    the_date = datetime.datetime.now()
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date = pre_date.strftime('%Y-%m-%d %H:%M:%S')  # 将日期转换为指定的显示格式
    pre_time = time.strptime(pre_date, "%Y-%m-%d %H:%M:%S")  # 将时间转化为数组形式
    pre_stamp = int(time.mktime(pre_time))  # 将时间转化为时间戳形式
    return pre_stamp
