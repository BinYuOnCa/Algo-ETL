"""2.	Get daily trading data (open, close, high, low and volume) from 2018-01-01 to 2020-12-31 for dt.
Plot candle stick plot and 30-day and 100-day exponential moving averages on one figure. [20 points]
(10 points for candle stick plot and 10 points for moving averages)"""
import mplfinance as mpf
from cycler import cycler
import matplotlib as mpl
import stock_assginment2_utils as utils
import matplotlib.pyplot as plt


"""
Get daily trading data (open, close, high, low and volume) from 2018-01-01 to 2020-12-31 for dt

"""
symbol = "THRM"
from_t = "2018-01-01 09:30:00"
to_t = "2020-12-31 21:00:00"
sql = f"select * from stock_candles_day where symbol='{symbol}' and dt>='{from_t}' and dt<='{to_t}' order by t "
dt = utils.query_dt(sql)
print(dt)

"""
30-day and 100-day exponential moving averages on one figure
用mplfinance 画candle stick plot图
"""
dt = dt[['o', 'h', 'l', 'c', 'v', 'dt']]
print('='*30)
print(dt)
#坑，必须用固定列名字，不能用其他列名，所以特意转换
dt.rename(
            columns={
            'dt': 'Date', 'o': 'Open',
            'h': 'High', 'l': 'Low',
            'c': 'Close', 'v': 'Volume'},
            inplace=True)
dt.index = dt['Date'] #索引必须是标准日期格式
dt = dt.drop('Date', axis=1)  #只需要o,h,l,c,v五个数据列，不删除多余的列也不会报错
print('-'*30)
print(dt)
symbol = 'dt'
# 设置基本参数
# type:绘制图形的类型，有candle, renko, ohlc, line等
# 此处选择candle,即K线图
# mav(moving average):均线类型,此处设置30,100日线
# volume:布尔类型，设置是否显示成交量，默认False
# title:设置标题
# y_label:设置纵轴主标题
# y_label_lower:设置成交量图一栏的标题
# figratio:设置图形纵横比
# figscale:设置图形尺寸(数值越大图像质量越高)
kwargs = dict(
    type='candle',
    mav=(30, 100),
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

# 设置均线颜色
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
         savefig='A_stock-%s_candle_day_line' % (symbol) + '.jpg')
plt.show()
