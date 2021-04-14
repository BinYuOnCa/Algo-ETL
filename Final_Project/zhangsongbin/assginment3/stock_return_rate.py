# 如下程序实现n+1算法后，计算回报率
import sys
import pandas as pd
import numpy as np
from utils import stock_time as myt
from utils import stock_pgfunctions as pg

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)


class ReturnRate():
    def __init__(self, symbol_df, start_date: int, end_date: int, period: list):
        self.symbol_df = symbol_df   # 这是股票代码的DataFrame格式，不是列表格式
        self.start_date = start_date
        self.end_date = end_date
        self.period = period

    def three_plus_one_p(self, df, date_ori: str) -> bool:
        """
        步骤5
        3+1 model(period and d0 as parameters ),  period2_high>period1_high,period2_low>period1_low
        :param df:  DataFrame 股票daily数据集
        :param date_ori: 这是D0的日期
        :return: True 或 False
        """
        dd = df.loc[df['date'] == date_ori].index
        date_ori_idx = dd[0]  # 这个出来才是一个integer
        d3_days = self.period[0]
        d2_days = self.period[1]
        d1_days = self.period[2]
        d1_high = df['h'].iloc[(date_ori_idx - d1_days-d2_days-d3_days):(date_ori_idx - d1_days-d2_days)].max()
        d2_high = df['h'].iloc[(date_ori_idx - d1_days-d2_days):(date_ori_idx - d1_days)].max()
        d3_high = df['h'].iloc[(date_ori_idx - d1_days):date_ori_idx].max()
        d1_low = df['l'].iloc[(date_ori_idx - d1_days-d2_days-d3_days):(date_ori_idx - d1_days-d2_days)].min()
        d2_low = df['l'].iloc[(date_ori_idx - d1_days-d2_days):(date_ori_idx - d1_days)].min()
        d3_low = df['l'].iloc[(date_ori_idx - d1_days):date_ori_idx].min()
        # 这里是重点，逻辑判断，每天最高价都高于前一天的最高价，每天的最低价也高于前一天的最低价
        result = (d1_high < d2_high < d3_high and d1_low < d2_low < d3_low)
        return result

    def three_plus_one_return(self, df, stock: str, date_ori: str):
        """
        步骤4： 输入数据集和不同阶段的日期，计算回报率
        :param df: DataFrame 股票daily数据集
        :param stock: str 具体某个股票的代码
        :param date_ori: str  这是D0的日期
        :return: float  回报率
        """
        try:
            dd = df.loc[df['date'] == date_ori].index
            date_ori_idx = dd[0]
            d4_days = self.period[-1]
            d0_price = df['c'].iloc[date_ori_idx]
            d4_price = df['c'].iloc[date_ori_idx + d4_days]
            return_rate1 = np.log(d4_price/d0_price)/d4_days   #这里时候要除以d4天，需要讨论下
            return return_rate1
        except:
            print(f"股票{stock}出错啦,开始日期：{self.start_date}=结束日期：{self.end_date}=D0日期点：{date_ori}")
            print(f"数据集如下:{df}")
            print(f"dd==={dd},date_ori_idx={date_ori_idx}, d0_price={d0_price}")
            print(f"d4_price={d4_price}")
            sys.exit(1)

    def validate_three_plus_one(self, stock: str):
        """
        步骤3： 蠕动，计算回报率。
        :param stock: str 是validate_three_plus_one_stocks输入的具体一个股票的代码
        :return: 利润率的数据集
        """
        # 为了避免蠕动后，最后一段后面无法取数，我特意给了额外增加两端的数据集
        st1 = myt.dateint_stamp(self.start_date, "begin")
        d3_days = int(self.period[0])
        d2_days = int(self.period[1])
        d1_days = int(self.period[2])
        d4_days = int(self.period[-1])  # 这是d4周期，即列表最后一个数字
        date_temp = myt.add_dateint(self.end_date, 2*d4_days)  # 累加上两端D4的天数
        et1 = myt.dateint_stamp(date_temp, "end")
        # print(f"d4_days={d4_days},date_temp={date_temp},df1的结束时间戳et1==={et1}")
        # 下面这段拿的是开始日期到特殊结束日期（你一开始输入日期后在后推两个D4的日期）的数据集，名字叫df1
        sql1 = f"select * from stock_candles_day where symbol='{stock}' and t>={st1} and t<={et1} "
        df1 = pg.database_to_pd(sql1)
        df_index_list = list(range(0, len(df1.index)))
        df1.index = pd.Series(df_index_list)
        # 为了避免取不到数字，所以这里不用最早一开始的end_date,而是用end_date+2*period
        conn = pg.connect()
        list1 = myt.get_valid_dates(conn, stock, "day", self.start_date, date_temp)
        conn.close()
        df1["date"] = list1
        # 下面这段仅仅是为了拿到开始日期到正常结束日期（就是你一开始输入的结束日期）的数据集DataFrame，名字叫df2
        # 然后仅仅是为了知道这个df2数据集的长度len_df2而已
        st2 = myt.dateint_stamp(self.start_date, "begin")
        et2 = myt.dateint_stamp(self.end_date, "end")
        sql2 = f"select * from stock_candles_day where symbol='{stock}' and t>={st2} and t<={et2} "
        df2 = pg.database_to_pd(sql2)
        len_df2 = len(df2.index)
        # 为了避免日期不准（放假或周末啥的），我们用索引来推周期
        # 这里开始d3-d2-d1-d0是空白的，所以要隔了几个才能开始
        start_ind = d1_days + d2_days + d3_days
        end_ind = len_df2 - d4_days  # 注意，这里必须减去，否则会导致date_ori停留位置过于后面，然后再加D4就出错了
        result = []
        count = 0
        # 针对这单个股票，我们开始在数据集里逐个蠕动测试
        for ind in range(start_ind, end_ind):
            date_ori = df1['date'].iloc[ind]
            # print(f"ind={ind}, date_ori={date_ori}")
            criteria = self.three_plus_one_p(df1, date_ori)
            if criteria:  # 只有价格有增长才会被记录下来，否则为空
                return_rate2 = self.three_plus_one_return(df1, stock, date_ori)
                result.append(return_rate2)
                count += 1
        if count > 0:
            return_array = np.array(result)
            return_avg_np = np.nanmean(return_array)
            # print(f"{stock} has {count} instances following 3+1 model, and its average return_rate2 is {return_avg}.")
        else:
            return_avg_np = np.nan
        return return_avg_np  # 注意，如果这段时间都是跌，则这里会返回空值

    def validate_three_plus_one_stocks(self):
        """
        步骤2: 主要逻辑处理部分，实现针对股票列表的查询
        :return: ndarray
        """
        stocks = self.symbol_df['symbol'].values.tolist()
        return_list = []
        for stock in stocks:
            return_rate_all = self.validate_three_plus_one(stock)  # The average return_rate for each stock
            # print(f"{stock}股票回报率=={return_rate_all}")
            if not np.isnan(return_rate_all):
                return_list.append(return_rate_all)
        if len(return_list) > 0:
            return_array_s = np.array(return_list)
            return_avg_s = np.nanmean(return_array_s)  # 求平均数,注意这里是nanmean，去除空值的平均值
            # print('*' * 30)
            # print(f'这{len(stocks)}股票{stock_list}的平均回报率，\n average return rate是: {return_avg_s}.')
        else:
            return_avg_s = -1
        return return_avg_s


if __name__ == '__main__':
    s_date = 20200301     # 开始日期
    e_date = 20200828     # 结束日期
    # 假设每隔10天
    # 你也可以设置任何长度的列表来指定周期，这里举例用四个为一个周期d3--d2--d1--d0--d4
    p = [13, 10, 13, 13]
    symbol_list = ['IBM', 'TLSA']
    symboldf = pg.get_symbol_df(symbol_list, s_date, e_date)   # 各个股票的代码的一个dataframe数据集
    a = ReturnRate(symboldf, s_date, e_date, p)
    return_avg = a.validate_three_plus_one_stocks()
    if return_avg == -1:
        print(f"收益率都是负的")
    else:
        print(f"周期列表是：{p}，收益率是{return_avg}")


