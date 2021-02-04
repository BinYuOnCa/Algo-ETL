import finnhub
import time
import sys
from utils import stock_pgfunctions as pg
from utils import stock_other_functions as oth
from utils.stock_settings import Settings
from utils.stock_download_import import Etl
stock_settings = Settings()


class Profile(Etl):
    """
    This is used to download company profile  data and import to database
    Use API function
    """
    def __init__(self, option):
        super().__init__(option)
        self.table = stock_settings.profile_table
        self.path = stock_settings.profile_path
        self.title = stock_settings.profile_title
        pg.clear_table(self.table)
        self.columns = stock_settings.profile_title

    def download_finnhub(self):
        """
        Download company profile by API.
        :return: res:data or None
        """
        finnhub_client = finnhub.Client(api_key=stock_settings.api_key)
        try:
            res = finnhub_client.company_profile2(symbol=self.symbol_name)
        except (Exception, finnhub.exceptions.FinnhubAPIException) as error:
            print(f" API limit reached. error is: {error}")
            oth.log(error)
            sys.exit()
            return None
        except Exception as error:
            oth.log(error)
            return None
        return res


begin_time = int(time.time())
item = "company_profile"
# Inherit the parent class
dt = Profile(item)
dt.stock_download_import()
end_time = int(time.time())
print(f"This task spend  {round((end_time-begin_time)/60,2)} minutes.")
