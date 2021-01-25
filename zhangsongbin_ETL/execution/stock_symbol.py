import requests
import pandas as pd
from utils import stock_pgfunctions as pg
from utils import stock_other_functions as oth
import time
from utils.stock_settings import Settings


"""
This is used to download all symbol  data and import to database
Use URL get.
"""

stock_settings = Settings()
command = 'https://finnhub.io/api/v1/stock/symbol?exchange=US&token=' + stock_settings.api_key
r = requests.get(command)
# Convert the response object into a list of dictionaries
lst_dict_companies = r.json()
# Convert the list of dictionnaries into dataframes
df_companies = pd.DataFrame(lst_dict_companies)
df_companies.sort_values(by=["symbol"], ascending=True,
                         inplace=True, ignore_index=True)
# save as CSV
df_companies.to_csv(stock_settings.all_symbol_path, index=False, header=True)

df = pd.read_csv(stock_settings.all_symbol_path)
df = df.rename(
    columns={
    "currency": "currency",
    "description": "description",
    "displaySymbol": "displaySymbol",
    "figi": "figi",
    "symbol": "symbol",
    "type": "type"
    })
print("df===:\n", df)

# clear data of symbol table
pg.clear_table(stock_settings.all_symbol_table)
# import data
pg.insert7(stock_settings.all_symbol_table, df)


# send mail&sms
msg = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Download and Import " + stock_settings.all_symbol_table + " successfully.\n"
oth.send_sms(stock_settings.all_symbol_table, msg)
oth.send_email(stock_settings.all_symbol_table, msg)

