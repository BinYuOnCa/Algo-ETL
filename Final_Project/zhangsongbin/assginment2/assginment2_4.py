"""4.	Take 10 stocks in S&P500 and collect daily close price from 2020-06-01 to 2020-12-31 for selected stocks.
(1)	) Calculate daily return (return = log(today close/previous close)) for each of 10 stocks [5 points]
(2)	Run PCA on calculated daily return and find the first principal component. [15 points]
(3)	Plot first principal component and daily return of S&P500 in one figure[10 points]
(4)	Calculate the correlation coefficient between first principal component and daily return of S&P500 index. [10 points]"""
import stock_assginment2_utils as utils

from_t = "2018-01-01 09:30:00"
to_t = "2020-12-31 21:00:00"
stock10 = ['AEP','AMAZ','AMZN','C','CCL','COP','DELL','F','GE','IBM']
for i in stock10:
    print(i)
    utils.daily_return(i, from_t, to_t)