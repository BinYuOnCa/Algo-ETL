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
