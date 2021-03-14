import yfinance as yf
import numpy as np
from scipy import stats

# download
candles_df = yf.Ticker('THRM.V').history(interval='1d',
                                         start='2018-1-1',
                                         end='2021-1-1')
# log returns computation
candles_df['return'] = candles_df['Close'] / candles_df['Close'].shift(1)
candles_df['log_return'] = np.log(candles_df['return'])
# drop na, i.e., drop first day since no previous close based on the dataset
log_return = candles_df['log_return'].dropna()

# HP1: Shapiro-Wilk
shapiro_test_res = stats.shapiro(log_return)
print('The p-value for Shapiro-Wilk test is {}'.format(
    shapiro_test_res.pvalue))

# HP2: KS TEST
est_mean = log_return.mean()
est_sd = (log_return.var() * len(log_return) / (len(log_return) - 1))**.5

ks_test_res = stats.kstest(
    log_return, lambda x: stats.norm.cdf(x, loc=est_mean, scale=est_sd))
print('The p-value for K-S test is {}'.format(ks_test_res.pvalue))
