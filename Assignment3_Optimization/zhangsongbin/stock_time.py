import time
import datetime
import pandas as pd


def stamp_dateint(timestamp:int):
    """
    输入时间戳，得到简化的例如20210125格式的整数类型的值
    :param timestamp: time type
    :return: int
    """
    # 转换成localtime
    time_local = time.localtime(timestamp)
    # 转换成新的时间格式(2021-01-25 20:28:54)
    dt = time.strftime("%Y%m%d", time_local)
    dt_int = int(dt)
    return dt_int


def dateint_timestamp(date: int):
    """
    输入例如20210125格式
    :param date: int
    :return: time stamp int 类似1606798800
    """
    date1 = str(date)
    date2 = time.strptime(date1, "%Y%m%d")
    a = time.mktime(date2)
    #得到类似1606798800.0结果，然后去掉后两个，得到整数
    time1 = str(a)[:-2]
    return time1


def get_valid_dates(conn, security_symbol=None, interval="day", date_from=20210101, date_until=20210201):
    """
    输入股票代码，起始日期和终止日期，得到int型的时间的列表
    :param conn: pg.connect()
    :param security_symbol: str
    :param interval: day or min
    :param date_from: int 20210101
    :param date_until: int 20210201
    :return: int 时间， 且为列表
    [20210103, 20210104, 20210105, 20210106, 20210107]
    """
    res = list()
    date_from_stamp = dateint_stamp(date_from, "begin")
    date_until_stamp = dateint_stamp(date_until, "end")
    if security_symbol is not None:
        sql = f"select distinct dt from stock_candles_{interval} "\
            f"where symbol='{security_symbol}' "\
            f"and t>={date_from_stamp} and t<={date_until_stamp} "
    date_df = pd.read_sql(sql, conn)
    #date_df['dt']看起来像是字符串，其实是<class 'pandas._libs.tslibs.timestamps.Timestamp'>
    #注意这里，用.dt.strftime('%Y%m%d')把2021-01-03 19:00:00时间戳格式转为了20210103
    date_df['date'] = date_df['dt'].dt.strftime('%Y%m%d')
    date_df['date_int'] = date_df['date'].apply(lambda x: int(x))
    result = list(date_df['date_int'])
    return result



def dateint_stamp(date_int: int, style="begin"):
    """
    输入整数时间例如20210302 转成时间戳。
    如果要求开始日期，就自动加上0点。如果是结束日期 .就自动加上23点
    :param date: int 例如20210302
    :param style: str  例如"begin" 或者 "end"，默认"begin"
    return: 时间戳 1609718400
    """
    if style=="begin":
        date1 = str(date_int)+" 00:00:01"
    if style=="end":
        date1 = str(date_int)+" 23:59:59"
    date2 = time.strptime(date1, "%Y%m%d %H:%M:%S")
    a = time.mktime(date2)
    #得到类似1606798800.0结果，然后去掉后两个，得到整数
    timestamp = str(a)[:-2]
    return timestamp


def add_dateint(date_int: int, add_num: int = 1):
    """
    输入整数时间例如20210328, 输入要增加的天数例如4天，得到结果20210401
    如果天数为负数，就是减去天数
    :param date_int: int
    :param add_num: int
    :return: int
    """
    a = str(date_int)
    b = datetime.date(int(a[0:4]), int(a[4:6]), int(a[-2:]))
    # 加上要增加的天数
    c = b + datetime.timedelta(add_num)
    c_year = str(c.year)
    if c.month < 10:
        c_month = "0"+str(c.month)
    else:
        c_month = str(c.month)
    if c.day < 10:
        c_day = "0"+str(c.day)
    else:
        c_day = str(c.day)
    d = c_year + c_month + c_day
    return d

