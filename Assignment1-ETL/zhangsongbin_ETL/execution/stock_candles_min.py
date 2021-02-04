import finnhub
import time
from utils import stock_pgfunctions as pg
from utils import stock_other_functions as oth
from utils.stock_settings import Settings
from utils.stock_download_import import Etl
stock_settings = Settings()


class Minute_1(Etl):
    """
    This is used to download 1 minutes data and import to database
    Use API function
    """
    def __init__(self, option):
        super().__init__(option)
        self.table = stock_settings.min_table
        self.path = stock_settings.min_path
        self.candle_id = 1
        self.title = stock_settings.candle_title
        self.item = stock_settings.candle_item_1min
        self.columns = stock_settings.candle_title

    def download_finnhub(self):
        """
        Download minute data using API
        :return: res:data or None
        """
        #
        max_t = pg.max_t(self.table, self.symbol_name)
        remark = self.symbol_name
        try:
            finnhub_client = finnhub.Client(api_key=stock_settings.api_key)
        except ConnectionResetError as error:
            print(f"ConnectionResetError=:{str(error)}")
            oth.log(error, remark)
            return None
        except requests.exceptions.ReadTimeout as error:
            print(f"Connection Read Timeout=:{str(error)}")
            oth.log(error, remark)
            return None
        except requests.exceptions.ConnectionError as error:
            print(f"ConnectionError=:{str(error)}")
            oth.log(error, remark)
            return None
        except Exception as error:
            oth.log(error, remark)
            return None
        one_year_ago_time = oth.get_day_time(365)
        if max_t == 0:
            from_t = one_year_ago_time
        else:
            from_t = max_t+1
        to_t = from_t + 86400*25  # about one month
        # print(from_t, to_t)
        try:
            res = finnhub_client.stock_candles(self.symbol_name,self.item, from_t, to_t)
        except finnhub.exceptions.FinnhubAPIException as error:
            print(f"Bad gateway or limit reached. error=:{str(error)} ")
            oth.log(error, remark)
            return None
        except ValueError as error:
            print(f"invalid=:{str(error)}")
            oth.log(error, remark)
            return None
        except Exception as error:
            oth.log(error, remark)
            return None
        return res

if __name__ == '__main__':
    begin_time = int(time.time())
    # Inherit the parent class
    dt = Minute_1("1min")
    dt.stock_download_import()
    end_time = int(time.time())
    print(f"This task spend  {round((end_time-begin_time)/60,2)} minutes.")
