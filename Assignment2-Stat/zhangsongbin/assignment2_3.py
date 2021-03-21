"""3.Use the same data in Question 2.
(1) Calculate daily return (return = log(today close/previous close)) [5 points]
(2) Conduct the hypothesis testing to check if the distribution of daily return is normal.  [15 points]"""
import stock_assginment2_utils as utils


symbol = "THRM"
from_t = "2018-01-01 09:30:00"
to_t = "2020-12-31 21:00:00"
"""
(1) Calculate daily return (return = log(today close/previous close)) [5 points]
"""
utils.daily_return(symbol, from_t, to_t)

"""
(2) Conduct the hypothesis testing to check if the distribution of daily return is normal.  [15 points]
"""
utils.hyp_test_pic1(symbol, from_t, to_t)
utils.hyp_test_pic2(symbol, from_t, to_t)
result = utils.hyp_test_data(symbol, from_t, to_t)
print(result)

