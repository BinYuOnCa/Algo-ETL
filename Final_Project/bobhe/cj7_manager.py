import time
import pause
import threading
from ibapi.contract import Contract
from execution.robot_cj7 import IBapi
from execution.model1 import Model1Stock  # model 1
from execution.params import *
from utils.helpers import *
import datetime
from utils.db_util import *
import os.path
import sys


pd.options.mode.chained_assignment = None
app = None


def run_loop():
    app.run()


def execution_main(secs):
    global app
    global engine
    tracing_cnt = 0
    req_id = 0
    sec_positions = dict()
    today = datetime.datetime.today()
    print(f"today={today}")
    latest_data_date = get_latest_datadate(engine)
    #latest_data_date = 20210401
    print(f"latest_data_date={latest_data_date}")
    # get the security list with the dynamic buy power threshold for each security based on normalized ATR
    if os.path.isfile(SECURITIES_NATR_FILE):
        buy_thd_list = read_sec_list(SECURITIES_NATR_FILE)
        print(f"secs_buy_thd= {buy_thd_list}")
    else:
        # refer calculate_model1_buy_thd from db_util.py
        buy_thd_list = calculate_model1_buy_thd(engine, secs, latest_data_date, interval=14)
    # select the stock list fit for three plus one model , order by turnover amount descending
    if os.path.isfile(SECURITIES_LIST_FILE):
        selected_secs_with_turnover = read_sec_list(SECURITIES_LIST_FILE)
        print(f"selected_secs_with_turnover= {selected_secs_with_turnover}")
    else:
        # refer get_sec_list_fit_three_plus_one_model from db_util.py
        selected_secs_with_turnover = get_sec_list_fit_three_plus_one_model(engine, secs, latest_data_date, 10)
    for sec_with_turnover in selected_secs_with_turnover:
        if tracing_cnt < MAX_TRACING:
            new_contract = Contract()
            new_contract.symbol = sec_with_turnover['security_name']
            new_contract.secType = 'STK'
            new_contract.exchange = 'SMART'
            new_contract.currency = 'USD'
            if sec_with_turnover['security_name'] in PRY_XCH:
                new_contract.primaryExchange = PRY_XCH[sec_with_turnover['security_name']]
            req_id += 1
            for sec_buy_thd in buy_thd_list:
                if sec_buy_thd['security_name'] == sec_with_turnover['security_name']:
                    buy_thd = sec_buy_thd['buy_thd']
            # refer class Model1Stock from model1.py
            sec_positions[req_id] = Model1Stock(new_contract, buy_thd)
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
    # 1 for real time data
    # 3 for delay data
    app.reqMarketDataType(3)
    time.sleep(1)
    # refer refresh_vwap_bars from robot_cj7.py
    app.refresh_vwap_bars()
    for each in sec_positions:
        print(f"initializing security with buy_thd: {round(100 * sec_positions[each].buy_thd, 2)}% "
              f"for {sec_positions[each].contract.symbol}... ")
        app.reqMktData(each, sec_positions[each].contract, '', False, False, [])
        time.sleep(0.1)
    cancel_buy_datetime = datetime.datetime.combine(today, datetime.time(15 + TIME_ZONE_ADJ, 00, 00))
    clear_position_datetime = datetime.datetime.combine(today, datetime.time(15 + TIME_ZONE_ADJ, 59, 30))
    now = datetime.datetime.now()
    while now + datetime.timedelta(0, 60) < clear_position_datetime:
        next_minute = now + datetime.timedelta(0, 60)
        pause.until(next_minute)
        now = datetime.datetime.now()
        if now > cancel_buy_datetime:
            # Retrieving currently active orders
            app.ret_api_orders()
        # refresh vwap bar for every minute
        app.refresh_vwap_bars()
    app.clear_position()
    # end_of_day_analysis(today_int, end_of_day_stats)
    time.sleep(30)  # give some time for the system to fill the orders
    app.disconnect()


if __name__ == "__main__":
    engine = get_engine()
    secs = get_sp500_stock_synbols_list()
    # remove the turnover column , save it
    # secs = pd.read_csv('secs_list.csv')
    # secs = secs['security_name'].values.tolist()
    # print(secs)
    execution_main(secs)
