import yfinance as yf
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt

# Selected stocks
symbols = [
    'AAPL', 'AMZN', 'FB', 'GOOGL', 'JNJ', 'JPM', 'MA', 'MSFT', 'TSLA', 'V'
]
# Download
candles_df = yf.Tickers(symbols).history(interval='1d',
                                         start='2020-6-1',
                                         end='2021-1-1')
# Compute log return
log_return_df = pd.DataFrame()
for symbol in symbols:
    close_price = candles_df['Close', symbol]
    returns = (close_price / close_price.shift(1)).dropna()
    log_return_df[symbol] = np.log(returns)
log_return_df.reset_index(inplace=True)
# Run PCA
pca = PCA()
pca.fit(log_return_df.iloc[:, 1:])
# First Principle
log_return_df['p1'] = np.dot(log_return_df.iloc[:, 1:], pca.components_[0])
log_return_df['p1']
# Download
sp500_df = yf.Ticker('^GSPC').history(interval='1d',
                                      start='2020-6-1',
                                      end='2021-1-1')
# Compute log returns
sp500_log_return_df = np.log(sp500_df['Close'] /
                             sp500_df['Close'].shift(1)).dropna()

#Plot
log_return_df.plot(
    'Date',
    'p1',
    label=r'$1^{st}$ principle',
    title='Plot of Log Returns against Date(2020/6/2-2020/12/31)',
    ylabel='Log Return',
    color='blue')
sp500_log_return_df.plot(label='S&P 500', color='red', legend=True)
plt.show()
# Compute r
corr_matrix = np.corrcoef(log_return_df['p1'], sp500_log_return_df)
print('The Pearson Correlation Coefficient is {}'.format(corr_matrix[0, 1]))
