from datetime import date, datetime


class TimeUtils:
    def date_to_time(self, convert_date: date):
        return datetime(convert_date.year, convert_date.month, convert_date.day)
