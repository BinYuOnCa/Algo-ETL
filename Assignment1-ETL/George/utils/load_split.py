from datetime import datetime
import pandas as pd
from pathlib import Path
from dateutil.relativedelta import relativedelta

import config.config_parser as conf
import utils.api_timer as api_timer
import utils.convert_time_timestamp as ctt
import utils.db_func as db_func
import utils.finn_func as finn_func
import utils.msg_service as msg_service
import utils.sql_func as sql_func
import utils.api_timer as api_timer



try:
    timer = api_timer.api_timer()
    timer.api_timer_handler()

    db_conn = db_func.DB_Conn()
    conn = db_conn.create_connection()
    ticker_to_update = sql_func.get_ticker_to_update_df(conn)
    # ticker_to_update = pd.DataFrame(pd.Series(['AAPL', "AMZN", "bbb", "thdh"]), columns=["ticker"])
    ticker_count = len(ticker_to_update)
    for index, row in ticker_to_update.iterrows():
        ticker = row.ticker
        timer.api_timer_handler()
        split_df = finn_func.get_split_df(ticker,
                                          str((datetime.today() - relativedelta(years=25)).date()),
                                          str(datetime.today().date()))
        if split_df is not None:
            sql_func.insert_df_to_db_iter(split_df)
except Exception as e:
    print(e)