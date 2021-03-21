from datetime import datetime
from datetime import date


def convert_unix_date_format(dt):
    """
    conver YYYY-MM-DD to unix datetime fromat
    :param dt: (str) YYYY-MM-DD
    :return: (int) unix datetime format
    """
    return int(datetime.strptime(dt, "%Y-%m-%d").timestamp())


def get_today_date():
    """
    get today date
    :param:
    :return: (str) YYYY-MM-DD
    """
    return str(date.today())
