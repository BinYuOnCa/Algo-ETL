from datetime import datetime

def get_now_datetime() -> str:
    ''' 返回统一格式的string
    '''
    return datetime.today().strftime('%Y-%m-%d %H:%M:%S')
