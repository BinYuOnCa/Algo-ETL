from stock_gentic_algorithm import Gentic as GA

start_date = 20200101   # 测试数据开始日期
end_date = 20201228     # 测试数据结束日期
cycle_age = 3  # 你想生殖繁育几代
change_days = 8  # 颜色列表到真实列表的加减天数
you_want_group = 10  # 你想一开始产生多少组的颜色列表（原始人）
you_want_list_length = [3, 7]  # 给一个范围：你想要列表多长(即几个阶段), 至少3开始
change_d = [6, 8]   # 给一个范围：加减days时间测试.建议8左右
a = GA(start_date,
       end_date,
       cycle_age,
       change_days,
       you_want_group,
       you_want_list_length,
       change_d)

b = a.main()
print(b)
