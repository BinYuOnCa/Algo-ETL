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
import plotly.graph_objects as go


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
    Send sms. 好像不能用了，废了
    :param msg:
    :return: 1
    """
    try:
        account_sid = sms_id
        auth_token = sms_token
        client = Client(account_sid, auth_token)
        message = client.messages.create(
                             body=msg,
                             from_=sms_from,
                             to=sms_to
                         )
        print(message.sid)
    except:
        print("短信发不出去了")
        pass
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


def candle_stick(security_symbol, date_key_int, dt):
    """
    plot candle stick chart
    :param security_symbol: string
    :param data_key_int: int
    :return: picture
    """
    print(f"为{security_symbol}股票画截止{date_key_int}时间的蜡烛图...")
    fig = go.Figure(data=[
        go.Candlestick(x=dt['t'],
                       open=dt['o'],
                       high=dt['h'],
                       low=dt['l'],
                       close=dt['c'],
                       name="Candle Stick"),
        go.Scatter(x=dt['t'],
                   y=dt['VWAP'],
                   mode="lines",
                   name="VWAP"
        )
    ]
    )
    fig.update_layout(
        title=f"Daily Price Visualization For {date_key_int}",
        yaxis_title=f"{security_symbol}"
    )
    fig.show()


def candle(df, x:str, open:str, high:str, low:str, close:str):
    """
    画蜡烛图，未充分测试过
    :param df:
    :param x:
    :param open:
    :param high:
    :param low:
    :param close:
    :return:
    """
    import plotly.graph_objects as go
    import matplotlib.pyplot as plt
    fig = go.Figure(data=[go.Candlestick(x=df[x], open=df[open], high=df[high], low=df[low], close=df[close])])
    fig.show()


def number_binary(list_a):
    """
    输入可变长度的任意列表（里面必须是数字，且数字不大于3），得到对应的二进制字符串
    :param list_a: list 类似[1, 3, 2, 3]格式
    :return: str 类似 01111011 格式的二进制字符串
    """
    a_len = len(list_a)
    str_bin = ""
    for i in range(a_len):
        temp = bin(list_a[i]).replace('0b', '')
        if len(temp) == 1:
            temp = "0" + temp
        str_bin = str_bin + temp
    return str_bin


def binary_number(str_bin, step: int):
    """
    输入二进制字符串， 得到列表（里面必须是数字，且数字不大于3）
    :param str_bin: 类似 01111011 格式的二进制字符串
    :param step: 步长，你想将这个二进制字符串按每隔多长来切分
    :return: list 类似[1, 3, 2, 3]格式
    """
    a_list = []
    for j in range(step, (len(str_bin)+1), step):
        a = str_bin[(j-step): j]
        a_list.append(a)
    b_list = []
    for i in a_list:
        b = "0b" + i
        b_int = int(b, 2)
        b_list.append(b_int)
    return b_list


def find_in_list(word: str, my_list: list):
    """
    在list中查找某个字符或数字
    :param word: 你要搜的字符或数字
    :param my_list: 被查的列表
    :return: bool 真假
    """
    for i in my_list:
        result = False
        if i == word:
            result = True
            break
    return result

def tickType_meaning(tickType):
    """
    输入BI公司的价格代号，得到价格含义
    """
    if tickType == 66:
        return "DELAYED_BID"
    if tickType == 67:
        return "DELAYED_ASK"
    if tickType == 68:
        return "DELAYED_LAST"
    if tickType == 69:
        return "DELAYED_BID_SIZE"
    if tickType == 70:
        return "DELAYED_ASK_SIZE"
    if tickType == 71:
        return "DELAYED_LAST_SIZE"
    if tickType == 71:
        return "DELAYED_LAST_SIZE"
    if tickType == 72:
        return "DELAYED_HIGH"
    if tickType == 73:
        return "DELAYED_LOW"
    if tickType == 74:
        return "DELAYED_VOLUME"
    if tickType == 75:
        return "DELAYED_CLOSE"
    if tickType == 76:
        return "DELAYED_OPEN"
    if tickType == 14:
        return "OPEN"
    if tickType == 0:
        return "BID_SIZE"
    if tickType == 1:
        return "BID"
    if tickType == 2:
        return "ASK"
    if tickType == 3:
        return "ASK_SIZE"
    if tickType == 4:
        return "LAST"
    if tickType == 5:
        return "LAST_SIZE"
    if tickType == 6:
        return "HIGH"
    if tickType == 7:
        return "LOW"
    if tickType == 8:
        return "VOLUME"
    if tickType == 9:
        return "CLOSE"
    if tickType == 48:
        return "RT_VOLUME"