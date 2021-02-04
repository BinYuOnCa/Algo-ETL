import pandas as pd
from io import StringIO
import psycopg2
import finnhub
import db_utility as util
import logging
import time
import datetime
import logging



def initial_stockdata(csv_file, resolution, start_time, end_time, table_name):
    finnhub_client = finnhub.Client(api_key="bv4f2qn48v6qpatdiu3g")
    #symbols = pd.read_csv(csv_file, nrows=3).to_numpy()
    symbols = pd.read_csv(csv_file).to_numpy()
    try:
        conn = util.cursor_setup()
        cur = conn.cursor()

        for symbol in symbols:
            logging.info('Start of Initial (%s%%)' % (datetime.datetime.now().strftime("%B") ))
            logging.info(symbol[0])
            res = finnhub_client.stock_candles(symbol, resolution, start_time, end_time)

            time.sleep(1)
            if res['s'] == 'no_data':
                logging.info("The symbol " + symbol + " has no data")
            else:
                res.pop('s', None)

                df = pd.DataFrame(res)

                df.insert(0, 'symbol', symbol[0], allow_duplicates=True)

                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                cur.copy_from(buffer, table_name, sep=",")
                conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger()

    # Setup finnhub client
    dailytable = 'stock_daily'
    minutetable = 'stock_minute'
    finnhub_client = finnhub.Client ( api_key="bv4f2qn48v6qpatdiu3g" )

    sqlcommand = "CREATE TABLE IF NOT EXISTS " + dailytable + " (" \
                                                             "symbol   VARCHAR(50) NOT NULL, " \
                                                             "close    FLOAT NOT NULL, " \
                                                             "high     FLOAT NOT NULL, " \
                                                             "low      FLOAT NOT NULL, " \
                                                             "open     FLOAT NOT NULL, " \
                                                             "time     INT NOT NULL, " \
                                                             "volume   FLOAT NOT NULL) "

    util.execute_sql(sqlcommand)
    sqlcommand = "CREATE TABLE IF NOT EXISTS " + minutetable + " (" \
                                                              "symbol   VARCHAR(50) NOT NULL, " \
                                                              "close    FLOAT NOT NULL, " \
                                                              "high     FLOAT NOT NULL, " \
                                                              "low      FLOAT NOT NULL, " \
                                                              "open     FLOAT NOT NULL, " \
                                                              "time     INT NOT NULL, " \
                                                              "volume   FLOAT NOT NULL) "

    util.execute_sql(sqlcommand)
    
    initial_stockdata('sec_list_1000.csv', 'D', 979527600, 1610582400, dailytable)
    logging.info('END of Initial (%s%%)' % (datetime.datetime.now().strftime("%B")))
    initial_stockdata('sec_list_1000.csv', '1', 979527600, 1610582400, minutetable)
    logging.info('END of Initial (%s%%)' % (datetime.datetime.now().strftime("%B")))
    print(datetime.datetime.now())
    print("End of code.")

