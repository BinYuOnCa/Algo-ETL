# 如下程序实现遗传算法
import sys
import pandas as pd
import numpy as np
from itertools import combinations
from utils import stock_pgfunctions as pg
from utils import stock_other_functions as oth
from stock_return_rate import ReturnRate
conn = pg.connect()


class Gentic():
    def __init__(self, start_date=20200101,
                 end_date=20207101,
                 cycle_age=1,
                 change_days=8,
                 you_want_group=5,
                 you_want_list_length=[5, 6],
                 change_d=[8, 9],
                 stock=['IBM']
                 ):
        self.start_date = start_date  # 测试数据开始日期
        self.end_date = end_date      # 测试数据结束日期
        self.cycle_age = cycle_age    # 你想循环几代
        self.change_days = change_days  # 颜色列表到真实列表的加减天数
        self.you_want_group = you_want_group  # 你想产生多少组的颜色列表
        self.long = you_want_list_length  # 你想要列表多长(即几个阶段), 每个列表里面每个数字不会大于3
        self.d = change_d   # 加减days时间测试范围
        self.best = {"best_one": "", "best_return": 0, "age": ""}  # 设置一个最终结果
        self.symbol_list = stock  # 你想查询的各个股票的列表

    def main(self) -> dict:
        for you_want_list_length in range(self.long[0], self.long[1]):
            for change_days in range(self.d[0], self.d[1]):
                you_want_list = self.random_color(self.you_want_group, you_want_list_length)
                # 下面开始一个一个列表开始计算回报率
                df = pd.DataFrame(columns=["color_period", "period", "return_rate"])  # 初始化空的df
                for i_list in range(len(you_want_list)):
                    i_return_avg = you_want_list[i_list]
                    period, return_avg = self.one_list_return(i_return_avg)
                    # print(f"原始颜色列表是{you_want_list[i_list]}，周期列表是：{period}，收益率是{return_avg}")
                    if return_avg > 0:
                        df.loc[i_list] = [you_want_list[i_list], period, return_avg]
                # 淘汰掉回报率为负数的人
                df.drop(df[df["return_rate"] <= 0].index, inplace=True)
                df.drop( df['period'][df['period'].apply(lambda x : True  if 0 in x else False)].index, inplace=True)
                df.loc[df["period"].astype(str).drop_duplicates().index]  # 去重
                if df.shape[0] == 0:  # 由于收益率都是负的，所以实在没有东西可以输出
                    self.best["best_one"] = ""
                    self.best["best_return"] = 0
                    self.best["age"] = ""
                if df.shape[0] == 1:  # 由于只有一个，无法生孩子，算了
                    self.best["best_one"] = df.iat[0, 0]
                    self.best["best_return"] = df.iat[0, 2]
                if df.shape[0] > 1:
                    df2 = self.married_rate_array(df)  # 这里只是为了加一个权重和按回报率排序
                    # print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!原始人最佳是{df2.iat[0, 1]},最佳回报率是{df2.iat[0, 2]}")
                    df2.drop(['color_period'], inplace=True, axis='columns')
                    # df2列为['period', 'return_rate', 'weight']
                    # print(f"原始人的数据集和权重是：\n{df2} ")
                    # print("="*30)
                    for i in range(self.cycle_age):  # 这里是循环几代
                        # 加入候选人数据集后，得到含孩子收益率的孩子数据集
                        if df2.shape[0] > 1:
                            new_f = self.new_family_df(df2)
                            # 合并两个数据集。 得到['period', 'return_rate', 'weight']
                            merge_f = self.merge_two(df2, new_f)
                            # print(f"合并两组后得到：\n {merge_f}")
                            try:
                                if merge_f.iat[0, 1] > self.best["best_return"]:
                                    self.best["best_one"] = merge_f.iat[0, 0]
                                    self.best["best_return"] = merge_f.iat[0, 1]
                                    if merge_f.iat[0, 2] == -1:
                                        self.best["age"] = "孩子辈"
                                    else:
                                        self.best["age"] = "父辈"
                            except:
                                print(f"=df2={df2}\n new_f={new_f}\n")
                                sys.exit(1)
                            # print("截止第%d代最新的最佳组合是%s, 最佳回报率是%f, 为%s" %(i+1, best["best_one"], best["best_return"], best["age"]))
                            df2 = self.married_rate_array(merge_f, 1)
        return self.best

    @staticmethod
    def random_pick(people_list: list,
                    probabilities: list) -> list:
        """
        以指定的概率获取元素。
        输入以一个列表为基准概率，从另一个列表中随机获取元素。
        要求 some_list 的长度和 probabilities 的长度一致,以及所有元素的概率相加为1.0
        返回是some_list列表中的其中一个数值成员
        :param people_list: list 待配对的人的列表
        :param probabilities: list 待配对的人所对应的概率福利
        :return: 某个元素
        """
        try:
            x = np.random.uniform(0, 1)
            cumulative_probability = 0.0
            for item, item_probability in zip(people_list, probabilities):
                cumulative_probability += item_probability
                if x < cumulative_probability:
                    break
        except:
            print(f"=people_list={people_list},probabilities={probabilities} ")
            sys.exit(1)
        return item

    def pair(self, people_list: list, probabilities: list):
        """
        两两结婚配对。
        输入一个列表list,类似[1,2,3,4]，1代表1号人，2代表2号人，3代表3号人，4代表4号人。
        还输入一个相应对的概率wight的list，类似[0.410006, 0.378210, 0.134852, 0.076932]。
        然后这些人（例如这里有四个人）里面按照概率两两配对。通过combinations(b, 2)，自然不会出现自己和自己配对。
        但是会出现（1，2）和（2，1）的情况，我就当做正常情况了。类似两个人因为结合次数多导致生了孩子多。有时是父亲想要孩子，有时是母亲想要孩子。
        返回是一个列表，类似[... (1, 4), (1, 2), (1, 2), (2, 4), (1, 4), (1, 4)]
        :param people_list:list 待配对的人的列表
        :param probabilities: list 待配对的人，有多少概率资格能参与交配。回报率越高的，越有可能参与交配。
        :return: list 返回一个列表，这个列表里面还有好多个小元祖，每个元祖里面就是一对夫妻
        """
        # 以指定的概率获取元素 以一个列表为基准概率，从一个列表中随机获取元素
        people = []
        for ii in range(7):
            people.append(self.random_pick(people_list, probabilities))
        pair_list = []
        for pair_i in combinations(people, 2):  # 抽出2个，看看总共有多少种组合
            num_a = pair_i[0]
            num_b = pair_i[1]
            if num_a != num_b:
                # 如下调整，确保每个元组，一定是小的数字在前面，大的数字在后面。方便后面去重
                if num_a > num_b:
                    aa = num_b
                    num_b = num_a
                    ee = (aa, num_b)
                    pair_list.append(ee)
                else:
                    pair_list.append(pair_i)
        return pair_list

    @staticmethod
    def married_rate_array(my_df, my_id=0):
        """
        计算每种组合的各自的权重且按照回报率排序，优秀回报率的组合排在前面且给权重较大。
        :param my_df: DataFrame 列为["color_period", "period", "return_rate"]的数据集
        :param my_id: int  须0或者1，如果是1，则跳过排序步骤
        :return: DataFrame 带有weight新增列的数据集
        """
        if my_id == 0:
            my_df.sort_values("return_rate", ascending=False, inplace=True)
            my_df.reset_index(drop=True, inplace=True)
        sum_return_rate1 = my_df["return_rate"].sum()
        my_df['w1'] = my_df['return_rate'].apply(lambda x: x/sum_return_rate1)
        my_df['w2'] = my_df['w1'].apply(lambda x: x**2)
        sum_return_rate_2 = my_df['w2'].sum()
        my_df['weight'] = my_df['w2'].apply(lambda x: x/sum_return_rate_2)
        my_df.drop(['w1', 'w2'], axis=1, inplace=True)
        return my_df

    @staticmethod
    def number_transfer(list_a, num, direct):
        """
        输入颜色列表，得到真实周期列表
        yellow=0 (5) ,red=1 (6) ,green=2 (7) ,blue=3  (8)
        :param list_a: list 类似[0,1,2,3]
        :param num: int  你想要加减的数，一般考虑在8左右浮动
        :param direct: int   只能1或-1， 如果是1，表示计算方向从[0,1,2,3]到[8,9,10,11]。如果-1，表示计算方向[8,9,10,11]到[0,1,2,3]
        :return: list 类似[8,9,10,11]
        """
        a_len = len(list_a)
        list_b = []
        for i in range(a_len):
            if direct == 1:
                temp1 = list_a[i] + num
            if direct == -1:
                temp1 = list_a[i] - num
            temp2 = int(temp1)
            list_b.append(temp2)
        return list_b

    def one_list_return(self, one_list: list):
        """
        输入一个单个列表，得到这个列表各种复杂计算后的回报率
        :param one_list: list 这是类似[1, 3, 2, 3]格式的列表，里面每个数字代表颜色
        :return period_real: list  这是类似[6, 8, 6, 8]格式的列表，里面是不同阶段的天数，实现d3-d2-d1-d0-d4周期
        :return return_avg: float  按这个周期设计跑完后的回报率
        """
        period_real = self.number_transfer(one_list, self.change_days, 1)  # 间隔不固定，按照列表里的顺序数字分别取
        # print(f"输入是{one_list}, 二级制是{a_bin}, 转成period是{period_real}")
        try:
            symbol_df = pg.get_symbol_df(self.symbol_list, self.start_date, self.end_date)
        except:
            print(f"=={self.symbol_list}")
            sys.exit(1)
        # symbol = pg.get_symbol_df(self.start_date, self.end_date)
        # try:
        rr_class = ReturnRate(symbol_df, self.start_date, self.end_date, period_real)
        return_rr = rr_class.validate_three_plus_one_stocks()
        # except:
        #     print(period_real)
        #     sys.exit(1)
        return period_real, return_rr

    def one_list_return2(self, one_list: list):
        """
        输入一个单个列表，得到这个列表各种复杂计算后的回报率
        :param one_list: list 这是类似[6, 8, 6, 8]格式的列表
        :return return_rr: float  按这个周期设计跑完后的回报率
        """
        symbol_df = pg.get_symbol_df(self.symbol_list, self.start_date, self.end_date)
        # symbol = pg.get_symbol_df(self.start_date, self.end_date)
        rr_class = ReturnRate(symbol_df, self.start_date, self.end_date, one_list)
        return_rr = rr_class.validate_three_plus_one_stocks()
        return return_rr

    @staticmethod
    def random_color(num: int, my_len: int):
        """"
        产生三个(或多个组合，num个)列表，每个列表都是一个组合
        每个组合是随机的颜色(每个组合里暂时固定为4个数字，每个数字都代表一种颜色）
        yellow=0 ,red=1 ,green=2, blue=3
        注意，这里返回的是列表，且列表里面的数值，每个数字都不会大于3，这是考虑后我们还需要转为二进制，所以我们不能给大于3的数
        :param num: int   你想要生成几个列表
        :param my_len: int   每个列表里你想多长，例如4类似[6, 8, 6, 8]，5类似[6, 8, 6, 8, 9]，建议大于3
        :return color_arr: list  例如[0,1,2,3]
        """
        color_arr = []
        for i in range(num):
            temp1 = np.random.choice([0, 1, 2, 3], my_len)
            temp2 = list(temp1)
            color_arr.append(temp2)
        return color_arr

    @staticmethod
    def child_born(father_gene: str, mother_gene: str):
        """
        输入两个相同长度的列表(父亲和母亲的基因片段），然后各取一段基因生孩子。
        本程序没有对父母亲基因的长度做特殊要求，可变长。
        方法是：随机取父亲后面一段基因，相应取母亲前面一段基因，拼接成孩子基因，
        然后孩子基因中随机一个变异（1变0， 0变1）。
        最后返回孩子变异后的基因
        :param father_gene: str 父亲基因,格式类似00100000
        :param mother_gene: str 母亲基因,格式类似01111011
        :return:str 孩子的基因, 格式类似00000001
        """
        len_gene = len(father_gene)
        excerpt_f = np.random.choice(range(3, len_gene - 1))
        excerpt_m = len_gene - excerpt_f
        # print(f"父亲基因抽取后面的:{excerpt_f},母亲基因抽取前面的:{excerpt_m}")
        part_gene_f = father_gene[len_gene - excerpt_f:]
        part_gene_m = mother_gene[:excerpt_m]
        child_gene = part_gene_f + part_gene_m
        # print(f"孩子基因是{child_gene}")
        random_num = np.random.choice(range(len_gene))
        position = child_gene[random_num]
        # print(f"抽取孩子基因的第{random_num}个位置片段是{position}")
        if position == "1":
            position = "0"
        else:
            position = "1"
        child_gene_mutation = child_gene[:random_num] + position + child_gene[random_num + 1:]
        # print(f"孩子基因变异后是{child_gene_mutation}")
        return child_gene_mutation

    def family_new(self, mydf):
        """
        输入只有父亲和母亲的数据集，得到全家的数据集
        :param mydf: DataFrame， 格式为:["period", "return_rate","weight"]列
        :return: DataFrame 含父亲，父亲ID（数字越小优先级越高），母亲，母亲ID，孩子
                           列名为['father', 'father_id', 'mother', 'mother_id', 'child']
        """
        people_list = list(mydf.index)
        probabilities = list(mydf["weight"])
        family_fm = self.pair(people_list, probabilities)
        child_list = []
        father_list = []
        father_id_list = []
        mother_list = []
        mother_id_list = []
        for i in family_fm:
            father_g = oth.number_binary(self.number_transfer(mydf.iloc[i[0], 0], self.change_days, -1))
            mother_g = oth.number_binary(self.number_transfer(mydf.iloc[i[1], 0], self.change_days, -1))
            child_g = self.child_born(father_g, mother_g)
            child = oth.binary_number(child_g, 2)
            child_list.append(self.number_transfer(child, self.change_days, 1))
            father_list.append(mydf.iloc[i[0], 1])
            father_id_list.append(i[0])
            mother_list.append(mydf.iloc[i[1], 1])
            mother_id_list.append(i[1])
            # print(f"===父亲基因是{father_g},母亲基因是{mother_g}, 孩子基因是{child_g},孩子周期列表是{child}")
        my_family = pd.DataFrame(columns=["father", "father_id", "mother", "mother_id", "child"])
        my_family["father"] = father_list
        my_family["father_id"] = father_id_list
        my_family["mother"] = mother_list
        my_family["mother_id"] = mother_id_list
        my_family["child"] = child_list
        # print(my_family)
        return my_family

    def new_family_df(self, candidate):
        """
        输入要生孩子的候选人的数据集，得到新的家庭的数据集。新家庭包括候选人和孩子。孩子的数据集只有孩子。
        :param candidate: DataFrame 候选人的数据集。 列为['period', 'return_rate', 'weight']
        :return: DataFrame 孩子的数据集。  列为['child', 'return']
        """
        # 下面开始生娃并计算娃的收益率
        family = self.family_new(candidate)
        family["return"] = family["child"].apply(lambda x: self.one_list_return2(x))
        # 将孩子们的收益率按照高低排序
        family.sort_values("return", ascending=False, inplace=True)
        family.reset_index(drop=True, inplace=True)
        child_avg_rt = format(family["return"].mean(), '.7f')
        # print(f"!!!!!!!!!!!!!!!!!!!孩子最佳是{family.iat[0,4]},最佳回报率是{family.iat[0,5]},孩子第一代的平均收益率为{child_avg_rt}")
        family.drop(['father', 'father_id', 'mother', 'mother_id'], inplace=True, axis='columns')
        # print(f"生完孩子后的家庭数据集是：\n{family}")
        return family

    @staticmethod
    def merge_two(df_parent, df_child):
        """
        输入两个数据集，分别是父母一辈的数据集df_parent和孩子df_child的数据集。
        中间处理过程无非就是合并，排序，计算回报率中位数，然后淘汰掉低于中位数的，最后留下回报率高于中位数的人（含父母和孩子）
        注意，还要淘汰掉回报率为负数的人
        可以通过看weight数值来判断是父母辈的人，还是孩子辈的人。weight大于1的这里表示父母辈，weight=-1的表示孩子辈。正常孩子辈数量会比较占多数。
        :param df_parent: DataFrame 父母亲的数据集
        :param df_child: DataFrame  孩子辈的数据集   列为['child', 'return']
        :return: DataFrame  新的数据集排序，列为['period', 'return_rate', 'weight']
        """
        temp = df_child.copy()
        temp.columns = ['period', 'return_rate']
        temp["weight"] = -1
        df3 = temp.append(df_parent)
        # df3.reset_index(drop=True, inplace=True)  # 重置索引
        # 去除重复值。由于df无法对list去重，所以我们就将其转为str，然后去除对应的索引的数据
        df3.loc[df3["period"].astype(str).drop_duplicates().index]
        df3.sort_values("return_rate", ascending=False, inplace=True)
        df3.reset_index(inplace=True)
        # 淘汰掉回报率为负数的人
        df3.drop(df3[df3["return_rate"] <= 0].index, inplace=True)
        middle_num = df3.median()['return_rate']
        # 只保留回报率大于中位数的人
        df3.drop(df3[df3["return_rate"] <= middle_num].index, inplace=True)
        df3.drop(labels="index", axis=1, inplace=True)
        # print(df3)
        return df3


conn.close()
