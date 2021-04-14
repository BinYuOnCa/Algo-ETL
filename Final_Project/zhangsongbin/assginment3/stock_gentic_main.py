# 其实应该每天晚上在下载完全部最新数据后，跑一下这个，然后更新算法。如果全部股票，应该很慢。
from stock_gentic_algorithm import Gentic as GA
from utils import stock_pgfunctions as pg
from utils import stock_time as st
# sql = "select max(t) as max_date from stock_candles_day  group by symbol order by max(t) desc  limit 1"
# sd = pg.execute_sql(sql)   # 由于各个股票的最新数据日期可能不同，我取最早的那天，作为测试数据开始日期
# print(st.stamp_dateint(sd[0][0]))
# ed = st.add_dateint(sd, -180)   # 测试数据结束日期吗，我取半年前

# 这里做了局限性，只查找有2021年4月1日数据的股票的列表
sql = "select distinct symbol from stock_candles_day where extract(day from dt)=1 and  extract(year from dt)=2021 and  extract(month from dt)=4 and c>300"
stock = list(pg.database_to_pd(sql)["symbol"])
print(f"找到{len(stock)}只股票有2021年4月1日数据")
# stock = ['IBM', 'TLSA']

start_date = 20210115   # 测试数据开始日期
end_date = 20210401     # 测试数据结束日期
cycle_age = 2  # 你想生殖繁育几代
change_days = 8  # 颜色列表到真实列表的加减天数
you_want_group = 10  # 你想一开始产生多少组的颜色列表（原始人）
you_want_list_length = [3, 4]  # 给一个范围：你想要列表多长(即几个阶段), 至少3开始
change_d = [7, 9]   # 给一个范围：加减days时间测试.建议8左右
a = GA(start_date,
       end_date,
       cycle_age,
       change_days,
       you_want_group,
       you_want_list_length,
       change_d,
       stock
       )
result = a.main()
print(result)

# 把记录保存下来放到数据库表里
sql = f"insert into stock_algorithm values ('{result['best_one']}', '{result['best_return']}', now() )  "
pg.execute_insert_sql(sql)
print("存入数据完毕")

