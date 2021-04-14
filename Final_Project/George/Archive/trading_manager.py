import datetime
import time
import pause
import threading
import numpy as np  # 4.2
import pandas as pd
from ibapi.contract import Contract
from utils.api import IBapi
#from utils.model56 import Model56Stock  # model 56
#from utils.oscillation_floor_5 import *  # 5
from utils.params import *
from utils.helper import *
from utils.sql_func import get_latest_datadate, calculate_high
pd.options.mode.chained_assignment = None


app = None


def run_loop():
    app.run()


def execution_main(secs):
    global app
    tracing_cnt = 0
    req_id = 0
    sec_positions = dict()
    today = datetime.datetime.today()
    latest_data_date = get_latest_datadate()
    #secs = filter_securities(secs, min_thd=MIN_PRICE)
    #buy_thd_dict = calculate_m5_buy_thd(M52_KEY2, secs, start_date_key=20210126, end_date_key=latest_data_date,
    #                                    back_test=False)  # 5
    # high_price_dict = calculate_high(secs, start_date_key=20201001, end_date_key=latest_data_date)
    for sec in secs:
        if sec in AVOID_STOCKS:
            continue
        if sec in buy_thd_dict[latest_data_date] \
                and MAX_VIB >= np.abs(buy_thd_dict[latest_data_date][sec]) >= MIN_VIB \
                and tracing_cnt < MAX_TRACING:  # 5 trading cost deduction
            new_contract = Contract()
            new_contract.symbol = sec
            new_contract.secType = 'STK'
            new_contract.exchange = 'SMART'
            new_contract.currency = 'USD'
            if sec in PRY_XCH:
                new_contract.primaryExchange = PRY_XCH[sec]
            req_id += 1
            sec_positions[req_id] = Model56Stock()  # your model
            tracing_cnt += 1
    print(f"Registering {tracing_cnt} securities ... ")
    app = IBapi(sec_positions, TOTAL_BUY_POWER)
    app.connect(IBIP, PORT, 555)

    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()
    print("Starting thread...")
    time.sleep(1)  # Sleep interval to allow time for connection to server
    print("Retrieving buying power...")
    app.reqAccountUpdates(True, ACCOUNT)

    # Set market data type
    app.reqMarketDataType(1)
    time.sleep(1)

    for each in sec_positions:
        print(f"initializaing security with buy_thd: {round(100 * sec_positions[each].buy_thd, 2)}% for {sec_positions[each].contract.symbol}... ")
        app.reqMktData(each, sec_positions[each].contract, '', False, False, [])

    cancel_buy_datetime = datetime.datetime.combine(today, datetime.time(15 + TIME_ZONE_ADJ, 00, 00))
    clear_position_datetime = datetime.datetime.combine(today, datetime.time(15 + TIME_ZONE_ADJ, 59, 30))
    now = datetime.datetime.now()
    while now + datetime.timedelta(0, 60) < clear_position_datetime:
        next_minute = now + datetime.timedelta(0, 60)
        pause.until(next_minute)
        now = datetime.datetime.now()
        if now > cancel_buy_datetime:
            app.ret_api_orders()
        app.refresh_vwap_bars()

    app.clear_position()
    # end_of_day_analysis(today_int, end_of_day_stats)

    time.sleep(30)  # give some time for the system to fill the orders
    app.disconnect()


if __name__ == "__main__":
    securities = read_sec_list("stock_security.csv")
    execution_main(securities)
