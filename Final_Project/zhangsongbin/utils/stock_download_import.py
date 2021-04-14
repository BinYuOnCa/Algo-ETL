#-*-coding:utf-8-*-
import finnhub
import time
from utils import stock_pgfunctions as pg, stock_csv_functions as csv
from utils import stock_other_functions as oth
from utils.stock_settings import Settings


stock_settings = Settings()


class Etl():
    """
    It is a father class. Download and Import Day data to database.
    It is a daily function. And can be used by min data by inheritance.
    """

    def __init__(self, option):
        self.option = option
        self.candle_id = 0
        self.max_count = stock_settings.max_count
        if self.option == "day":
            self.table = stock_settings.day_table
            self.path = stock_settings.day_path
            self.candle_id = 1
            self.title = stock_settings.candle_title
            self.item = stock_settings.candle_item_D
            self.columns = stock_settings.candle_title
        self.symbol_name = ""

    def download_finnhub(self):
        """
        Only for daily.Search daily or minutes database to find max time.
        :return:res
        """
        max_t = pg.max_t(self.table, self.symbol_name)
        from_t = max_t + 86400  # the seconds of one day are 86400
        to_t = int(time.time())
        # print(f"下载的数据的股票是={self.symbol_name}==开始时间=={from_t} ===结束时间=={to_t} ")
        try:
            finnhub_client = finnhub.Client(api_key=stock_settings.api_key)
        except ConnectionResetError as error:
            print(f"ConnectionResetError={str(error)}")
            oth.log(error, self.symbol_name)
            return None
        except requests.exceptions.ReadTimeout as error:
            print(f"Connection Read Timeout=:{str(error)}")
            oth.log(error, self.symbol_name)
            return None
        except requests.exceptions.ConnectionError as error:
            print(f"ConnectionError=:{str(error)}")
            oth.log(error, self.symbol_name)
            return None
        except Exception as error:
            oth.log(error, self.symbol_name)
            return None
        try:
            res = finnhub_client.stock_candles(self.symbol_name, self.item, from_t, to_t)
        except finnhub.exceptions.FinnhubAPIException as error:
            print(f"Bad gateway or limit reached. error=:{str(error)} ")
            oth.log(error, self.symbol_name)
            return None
        except ValueError as error:
            print(f"invalid=:{str(error)}")
            oth.log(error, self.symbol_name)
            return None
        except Exception as error:
            print(f"other error=:{str(error)}")
            oth.log(error, self.symbol_name)
            return None
        return res

    def stock_download_import(self):
        """
        查询出所有的股票代码，然后for循环，逐个下载每个股票的历史日数据
        :return:
        """
        csv.clear_csv(self.path)
        pg_column_dt = pg.column(stock_settings.table_symbol, stock_settings.table_symbol_column)
        if pg_column_dt is not None:
            i = 0
            a_time = time.time()
            for symbol in pg_column_dt:
                self.symbol_name = symbol[0]
                res = self.download_finnhub()
                # print(f"下载的数据是====={res}")
                if (res is not None) and (res != {}) and (res != []):
                    csv.save_to_csv(self.candle_id, self.symbol_name, self.path, res)
                    if self.candle_id == 1:
                        if res["s"] != "no_data":
                            pg.pg_to_sql(self.table, self.path, self.columns)
                            print(f" Get {self.symbol_name},导入数据完毕.")
                        else:
                            print(f" {self.symbol_name} is no_data.")
                    else:
                        pg.pg_to_sql_company_profile(self.table, self.path, self.symbol_name)
                        print(f" Get {self.symbol_name},导入数据完毕.")
                else:
                    print(f" {self.symbol_name} has no data.")
                time.sleep(1)
            # send mail&sms
            msg = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) \
                + ": Download and Import " \
                + self.table + " successfully.\n"
            oth.send_sms(msg)
            # oth.send_email(self.table, msg)
        else:
            # send mail&sms
            msg = time.strftime(f"Don't find any symbol or has some problem in {self.table}.")
            oth.send_sms(msg)
            # oth.send_email(self.table, msg)


