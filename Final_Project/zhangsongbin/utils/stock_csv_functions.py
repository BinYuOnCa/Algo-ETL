import pandas as pd
import datetime as dt
from utils.stock_other_functions import log
from utils.stock_other_functions import send_sms
from io import StringIO
output = StringIO()
"""
There are the functions about CSV.
Clear csv content.
Add a title in a CSV file.
Save data frame to one CSV file
"""


def clear_csv(path):
    """
    Clear the content of csv.
    如已经有CSV文件，就清空文件，如没有文件则创建一个空CVS文件.
    目的是为后面的追加内容做准备.
    :param cvs file's path:
    :return:1 or None
    """
    try:
        a = open(path, mode='w')
    except Exception as error:
        send_sms(f"There is an error:{error}")
        log(error)
        return None
    a.truncate()
    a.close()
    return 1


def add_csv_title(path, title_list):
    """
    特意读取CSV然后为其添加标题，否则数据库无法准确导入
    :param path:
    :param title_list:
    :return:1 or None
    """
    # print(f"The path of CSV adding title is:{path}")
    try:
        add_column = pd.read_csv(path, header=None)
    except Exception as error:
        send_sms(f"There is an error:{error}")
        log(error)
        return None
    add_column.columns = title_list
    add_column.to_csv(path, index=False, header=True)
    return 1


def save_to_csv(candle_id, symbol, path, res):
    """
    Save to csv file, but may be it is easy to run error
    :param candle_id:int
    :param symbol:str
    :param path:str
    :param res:data
    :return:1 or None
    """
    if candle_id == 1:
        if res["s"] == "no_data":
            print(f"The {symbol} stock has not data.")
        else:
            try:
                candles = pd.DataFrame(res)
                candles['dt'] = [dt.datetime.fromtimestamp(x) for x in candles['t']]
                candles['symbol'] = symbol
                candles.to_csv(path, index=False, header=False, mode="a")
            except ValueError as error:
                print("类似https://finnhub.io/api/v1/stock/candle?symbol=ZYXI&resolution=D&from=1614349800&to=1614794055的错误。"\
                        "finnhub提供的数据质量有问题，比如两个C价格，T时间却给了三个，导致数据无法转成DataFrame格式。")
                send_sms(f"There is an error:{error},arrays must all be same length ")
                log(error)
                pass
            except Exception as error:
                send_sms(f"There is an error:{error}")
                log(error)
                return None
            return 1
    else:
        res['symbol'] = symbol
        profile = pd.DataFrame(res, index=[0])
        profile.to_csv(path, index=False, header=False, mode="a")
        return 1