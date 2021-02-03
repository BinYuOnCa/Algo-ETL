import os


class Settings():
    """
    All params:
    Databse, SMS, mail, title, count, items
    """
    def __init__(self):
        self.param_dic = {
            "host": "35.155.65.198",  # aws:35.155.65.198
            "database": "postgres",
            "user": "postgres"
        }

        self.param_dic["password"] = os.environ.get('param_dic_password')
        self.email_password = os.environ.get('email_password')
        self.sms_token = os.environ.get('sms_token')
        self.api_key = os.environ.get('api_key')

        # self.day_path = "d:/stock_candles_day.csv"
        # self.min_path = "d:/stock_candles_min.csv"
        # self.profile_path = "d:/stock_company_profile.csv"
        # self.all_symbol_path = "d:/stock_symbol.csv"
        # ###notice! to change when you upload to AWS!!
        self.day_path = "/tmp/stock_candles_day.csv"
        self.min_path = "/tmp/stock_candles_min.csv"
        self.profile_path = "/tmp/stock_company_profile.csv"
        self.all_symbol_path = "/tmp/stock_symbol.csv"

        self.day_table = "stock_candles_day"
        self.min_table = "stock_candles_min"
        self.profile_table = "stock_company_profile"
        self.all_symbol_table = "stock_symbol"

        self.table_symbol = "stock_symbol_thousand"
        # self.table_symbol = "stock_symbol"
        self.table_symbol_column = "symbol"

        self.email_server = "smtp.163.com"
        self.email_name = "zhangsongbin"
        self.email_from_addr = "xm1273471508@163.com"
        self.email_to_addr = "songbin.zhang@gmail.com"
        self.sms_id = "AC3efee82173552a23ccb91678392df34d"
        self.sms_from = "+12513517768"
        self.sms_to = "+15145586257"

        self.candle_title = [
                "c", "h", "l", "o",
                "s", "t", "v", "dt",
                "symbol"
            ]
        self.profile_title = [
                "country", "currency", "exchange", "finnhubindustry",
                "ipo", "logo", "marketcapitalization", "name",
                "phone", "shareoutstanding", "ticker", "weburl",
                "symbol"
        ]
        self.max_count = 55
        self.candle_item_D = "D"
        self.candle_item_1min = "1"
        self.candle_item_5min = "5"
        self.candle_item_15min = "15"
        self.candle_item_30min = "30"
        self.candle_item_60min = "60"
        self.candle_item_week = "W"
        self.candle_item_month = "M"

