from datetime import datetime
from dateutil import tz
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta


"""to convert between UTC timestamp to EST datetime"""


def convert_datetime_timestamp(_datetime = None):
    if _datetime is None:
        return int(datetime.timestamp(datetime.now()))
    elif type(_datetime) == str:
        return int(datetime.timestamp(parse(_datetime)))
    elif isinstance(_datetime, datetime) or isinstance(_datetime, datetime.date()):
        return int(datetime.timestamp(_datetime))
    else:
        return None

def convert_timestamp_datetime(timestamp=int(datetime.timestamp(datetime.now()))):
    # # Auto-detect zones:
    # return datetime.fromtimestamp(timestamp).replace(tzinfo=tz.tzlocal())
    return datetime.fromtimestamp(timestamp)

def get_past_date(years=25, months=0, days=0, _date=datetime.today()):
    if type(_date) == str:
        return parse(_date) - relativedelta(years=years, months=months, days=days)
    elif isinstance(_date, datetime):
        return _date - relativedelta(years=years, months=months, days=days)

class date_by_month_list:
    """
    A class call to generate a list of start and finish date
    return ["2020-01-17","2020-02-17"]
    call next method to get next list
    *** will generate one extra None for the output. need to filter it out.
    """

    def __init__(self, from_date, to_date):
        self.date_list = []
        self.from_date = parse(from_date) if isinstance(from_date, str) else from_date
        self.to_date = parse(to_date) if isinstance(to_date, str) else to_date

    def next_list(self):
        if self.from_date <= self.to_date:
            if self.to_date - relativedelta(months=1) <= self.from_date:
                self.date_list = [str(self.from_date.date()), str(self.to_date.date())]
                self.from_date = (self.from_date + relativedelta(months=1))
            else:
                self.date_list = [str(self.from_date.date()), str((self.from_date + relativedelta(months=1)).date())]
                self.from_date = (self.from_date + relativedelta(months=1))
            return self.date_list
        else:
            self.date_list = None

    def next_timestamp_list(self, month):
        """

        :param month: int how many months in between the start and end
        :return:
        """

        if self.from_date <= self.to_date:
            if self.to_date - relativedelta(months=month) <= self.from_date:
                self.timestamp_list = [convert_datetime_timestamp(self.from_date),
                                       convert_datetime_timestamp(self.to_date)]
                self.from_date = (self.from_date + relativedelta(months=month))
            else:
                self.timestamp_list = [
                    convert_datetime_timestamp(self.from_date),
                    convert_datetime_timestamp((self.from_date + relativedelta(months=month)))]
                self.from_date = (self.from_date + relativedelta(months=month))
        else:
            self.timestamp_list = None
        return self.timestamp_list

