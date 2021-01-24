from datetime import datetime
import pandas as pd

import utils.load_candles as load_candles
import utils.api_timer as api_timer
import utils.db_func as db_func
import utils.sql_func as sql_func
import utils.helper as helper
from utils.msg_service import send_sms
from utils.msg_service import send_email


def algo_etl_main_control(candle_freq_list=["D"]):
    try:
        start_time = datetime.today()
        timer = api_timer.api_timer()
        ticker = ""
        ticker_success = 0
        error_num = 0
        error_flag = 0
        # candle_freq_list = ["D", 1]
        ticker_no_data = ""
        ticker_error = ""
        db_conn = db_func.DB_Conn()
        conn = db_conn.create_connection()
        for candle_freq in candle_freq_list:
            if candle_freq == "D":
                table_name = "us_equity_finn_daily"
            elif candle_freq == 1:
                table_name = "us_equity_finn_1m"
            ticker_no_data = ticker_no_data + "\n\n" + str(candle_freq) + ": \n"
            ticker_error = ticker_error + "\n\n" + str(candle_freq) + ": \n"
            ticker_to_update = sql_func.get_ticker_to_update_df(conn)
            current_last_timestamps_df = sql_func.get_last_timestamp_df(table_name, conn)
            current_last_timestamps_df.set_index('symbol', inplace=True)
            # ticker_to_update = pd.DataFrame(pd.Series(['AAPL', "AMZN", "bbb", "thdh"]), columns=["ticker"])
            ticker_count = len(ticker_to_update)
            for index, row in ticker_to_update.iterrows():
                ticker = row.ticker
                last_timestamp = helper.get_last_timestamp(ticker, current_last_timestamps_df)
                execution_result = load_candles.load_candles(row.ticker, table_name, candle_freq, timer, last_timestamp, conn)
                if execution_result == "Success":
                    ticker_success += 1
                elif execution_result == "no data":
                    ticker_no_data = ticker_no_data + str(row.ticker) + ","
                else:
                    ticker_error = ticker_error + str(row.ticker) + ","

        if ticker_no_data.count(",") != 0:
            error_flag = 1
            ticker_no_data = ticker_no_data[:-1]
            error_num = error_num + ticker_no_data.count(",")
        if ticker_error.count(",") != 0:
            error_flag = 1
            ticker_error = ticker_error[:-1]
            error_num = error_num + ticker_error.count(",")
    except Exception as e:
        try:
            error_sql = "insert into error_rec (error_time, error_info) values ('" \
                        + str(datetime.today()).replace("'", "''") + \
                        "', 'Error when getting data with ticker [" \
                        + str(ticker) + "]: " + str(e).replace("'", "''") + "')"
            with conn.cursor() as cursor:
                cursor.execute(error_sql)
        except Exception as e2:
            print('"', datetime.today(),
                  '" , "Error when adding error message to database: ' + str(e2) + '", "'
                  + 'Original Error when getting data with ticker \'', str(ticker), '\': ', e, '"')

    finally:
        if error_flag == 0:
           send_sms("Algo-Trading executed correctly, Started at: " + str(start_time) + "\n Finished at: "
                    + str(datetime.today()) + "\ntickers processed: " + str(ticker_success))
           send_email("Algo-Trading executed correctly", "Started at: " + str(start_time) + "\n Finished at: "
                      + str(datetime.today()) + "\ntickers processed: "
                      + str(ticker_success)+ "\n Algo trading executed successfully")
        else:
            if execution_result == "Success":
                send_email("Algo-Trading executed finished with " + str(error_num) + " error(s)!",
                           "Algo-Trading executed finished with " + str(error_num)
                           + " error(s)! \nStarted at: " + str(start_time) + "\n Finished at: "
                           + str(datetime.today()) + "\ntickers processed: " + str(ticker_success)
                           + "\n tickers list cannot update: " + str(ticker_no_data))

                send_sms("Algo-Trading execution finished with " + str(error_num)
                         + " error(s), \nStarted at: " + str(start_time) + "\n Finished at: "
                        + str(datetime.today()) + "\ntickers proceessed: " + str(ticker_success))
            else:
                send_email("Algo-Trading executed finished with " + str(error_num)
                           + " error(s)!", "Algo-Trading executed finished with " + str(error_num)
                           + " error(s)! \nStarted at: " + str(start_time) + "\n Finished at: "
                           + str(datetime.today()) + "\ntickers processed: " + str(ticker_success)
                           + "\n tickers list cannot update: " + str(ticker_no_data)
                           + '\nLast Error detail: "' + str(execution_result))
                send_sms("Algo-Trading execution finished with " + str(error_num)
                         + " error(s), \nStarted at: " + str(start_time) + "\n Finished at: "
                         + str(datetime.today()) + "\ntickers processed: " + str(ticker_success)
                         + "\nLast error detail " + str(execution_result))


if __name__ == "__main__":
   algo_etl_main_control(["D", 1])