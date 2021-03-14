import math
import numpy as np
import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt
from statsmodels import api as sm
from utils import stock_pgfunctions as pg
import plotly.graph_objects as go
import mplfinance as mpf
from cycler import cycler
import matplotlib as mpl


def query_dt(sql):
    """
    连接数据库，查询指定SQL语句，返回PD的数据集
    :param sql: str
    :return: panda data
    """
    conn = pg.connect()
    dt = pd.read_sql(sql, conn)
    return dt


def vwap_pic(symbol, from_t, to_t):
    """
    用plot画VWAP图
    成交量加权平均价VWAP是将多笔交易的价格按各自的成交量加权而算出的平均价
    """
    sql = f"select * from stock_candles_min where symbol='{symbol}' and dt>='{from_t}' and dt<='{to_t}'"
    dt = query_dt(sql)
    dt['avg_p'] = ( dt['h'] + dt['l'] + dt['c'] ) / 3
    dt['v_cum'] = dt['v'].cumsum()
    dt['pv'] = dt['avg_p']*dt['v']
    dt['pv_cum'] = dt['pv'].cumsum()
    dt['VWAP'] = dt['pv_cum']/dt['v_cum']
    print(dt.head())
    #打印出每日闭市价格（绿线）和VMAP价格（橙线）
    plt.figure(figsize=(12, 8))
    plt.plot(dt['dt'], dt['c'], color='green', label="Close Price")
    plt.plot(dt['dt'], dt['VWAP'], color='orange', label="VWAP")
    plt.title('VWAP and close price of dt on 21Jan.2020', fontsize=20)
    plt.xlabel('Time', fontsize=16)
    plt.ylabel('Price', fontsize=16)
    plt.legend()
    plt.show()


def candle_stick_plot1(symbol, from_t, to_t):
    """
    画candle stick plot图，方式一
    用plotly.graph_objects
    """
    sql = f"select * from stock_candles_min where symbol='{symbol}' and dt>='{from_t}' and dt<='{to_t}'"
    dt = query_dt(sql)
    fig = go.Figure(data=[go.Candlestick(x=dt['dt'], open=dt['o'], high=dt['h'], low=dt['l'], close=dt['c'])])
    fig.show()


def candle_stick_plot2(symbol, from_t, to_t):
    """
    画candle stick plot图，方式二
    mplfinance 画
    """
    sql = f"select * from stock_candles_min where symbol='{symbol}' and dt>='{from_t}' and dt<='{to_t}'"
    dt = query_dt(sql)
    dt = dt[['o', 'h', 'l', 'c', 'v', 'dt']]
    #坑，必须用固定列名字，不能用其他列名，所以特意转换
    dt.rename(
                columns={
                'dt': 'Date', 'o': 'Open',
                'h': 'High', 'l': 'Low',
                'c': 'Close', 'v': 'Volume'},
                inplace=True)
    dt.index = dt['Date'] #索引必须是标准日期格式
    dt = dt.drop('Date', axis=1)  #只需要o,h,l,c,v五个数据列，不删除多余的列也不会报错
    # print('-'*30)
    # print(dt)
    symbol = 'dt'
    # 设置基本参数
    # type:绘制图形的类型，有candle, renko, ohlc, line等
    # 此处选择candle,即K线图
    # mav(moving average):均线类型,此处设置7,30,60日线
    # volume:布尔类型，设置是否显示成交量，默认False
    # title:设置标题
    # y_label:设置纵轴主标题
    # y_label_lower:设置成交量图一栏的标题
    # figratio:设置图形纵横比
    # figscale:设置图形尺寸(数值越大图像质量越高)
    kwargs = dict(
        type='candle',
        mav=(7, 30, 60),
        volume=True,
        title='\nUS_stock %s candle_line' % (symbol),
        ylabel='OHLC Candles',
        ylabel_lower='Shares\nTraded Volume',
        figratio=(15, 10),
        figscale=5)
    # 设置marketcolors
    # up:设置K线线柱颜色，up意为收盘价大于等于开盘价
    # down:与up相反，这样设置与国内K线颜色标准相符
    # edge:K线线柱边缘颜色(i代表继承自up和down的颜色)，下同。详见官方文档)
    # wick:灯芯(上下影线)颜色
    # volume:成交量直方图的颜色
    # inherit:是否继承，选填
    mc = mpf.make_marketcolors(
        up='green',
        down='red',
        edge='i',
        wick='i',
        volume='in',
        inherit=True)
    # 设置图形风格
    # gridaxis:设置网格线位置
    # gridstyle:设置网格线线型
    # y_on_right:设置y轴位置是否在右
    s = mpf.make_mpf_style(
        gridaxis='both',
        gridstyle='-.',
        y_on_right=False,
        marketcolors=mc)
    # 设置均线颜色，配色表可见下图
    # 建议设置较深的颜色且与红色、绿色形成对比
    # 此处设置七条均线的颜色，也可应用默认设置
    mpl.rcParams['axes.prop_cycle'] = cycler(
        color=['dodgerblue', 'deeppink',
               'navy', 'teal', 'maroon', 'darkorange',
               'indigo'])
    # 设置线宽
    mpl.rcParams['lines.linewidth'] = .5
    # 图形绘制
    # show_nontrading:是否显示非交易日，默认False
    # savefig:导出图片，填写文件名及后缀
    mpf.plot(dt,
             **kwargs,
             style=s,
             show_nontrading=False,
             savefig='A_stock_%s_candle_min_line' %(symbol) + '.jpg')
    plt.show()
    print("A_stock_%s_candle_min_line" %(symbol) + ".jpg" +"蜡烛图像已经画好。")

def daily_return(symbol, from_t, to_t):
    """
    计算日收益率daily_return
    计算公式是 ： return = log(today close/previous close)
    :param symbol: str
    :param from_t: str
    :param to_t: str
    :return: picture
    """
    sql = f"select * from stock_candles_day where symbol='{symbol}' and dt>='{from_t}' and dt<='{to_t}' order by symbol,series"
    dt = query_dt(sql)
    rt=[]
    rt.append(np.nan)
    for j in range(1, len(dt['c'])):
        r = (math.log((dt['c'][j])/(dt['c'][j - 1])))
        rt.append(r)
    dt['return'] = rt
    plt.plot(dt['dt'], dt['return'], '--')
    return plt.show()


def hyp_test_pic1(symbol, from_t, to_t):
    """
    画出正态分布的图。这是方法一。
    先计算对数收益率，然后画图
    :param symbol: str
    :param from_t: str
    :param to_t: str
    :return: picture
    """
    sql = f"select * from stock_candles_day where symbol='{symbol}' and dt>='{from_t}' and dt<='{to_t}' order by symbol,series"
    dt = query_dt(sql)
    #计算对数收益率
    a = np.log(dt['c'].pct_change()+1)
    #画出正态分布图
    a.hist(bins=50, figsize=(10, 6))
    return plt.show()

def hyp_test_pic2(symbol, from_t, to_t):
    """
    画出检验正态分布的图。这是方法二。
    X轴理论分位数，y轴样本分位数.只要不在一条直线上，就表示不符合正态分布
    :param symbol: str
    :param from_t: str
    :param to_t: str
    :return: picture
    """
    sql = f"select * from stock_candles_day where symbol='{symbol}' and dt>='{from_t}' and dt<='{to_t}' order by symbol,series"
    dt = query_dt(sql)
    #计算对数收益率
    a = np.log(dt['c'].pct_change()+1)
    fix, axes = plt.subplots(1, 1, figsize=(10, 12))
    sm.qqplot(a.dropna(), line='s', ax=axes)
    axes.set_title("hypothesis testing") #用中文做标题会出错
    return plt.show()

def hyp_test_data(symbol, from_t, to_t):
    """
    计算出检验正态分布的统计数据。这是方法三。
    :param symbol: str
    :param from_t: str
    :param to_t: str
    :return:str
    """
    sql = f"select * from stock_candles_day where symbol='{symbol}' and dt>='{from_t}' and dt<='{to_t}' order by symbol,series"
    dt = query_dt(sql)
    a = np.log(dt['c'].pct_change()+1)#计算对数收益率
    b = a.drop(0, axis=0)
    u = b.mean()  # 计算均值
    std = b.std()  # 计算标准差
    """
    kstest方法中的参数分别是：待检验的数据，检验方法（这里设置成norm正态分布），均值与标准差
    返回两个值：statistic → D值，pvalue → P值
    当p值大于0.05，说明待检验的数据符合为正态分布    
    """
    c = b.values.tolist()
    result = stats.kstest(c,'norm', (b, std))
    return result
