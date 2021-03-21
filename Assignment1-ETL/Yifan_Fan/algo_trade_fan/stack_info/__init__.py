import pandas as pd
from os.path import join, dirname
from threading import Lock

total_lock = Lock()

csv_path = join(dirname(__file__), 'sec_list_1000.csv')
STACK_LIST = pd.read_csv(csv_path)

check_path = join(dirname(__file__), 'stack_no_intraday.csv')
STACK_NO_DATA = pd.read_csv(check_path)
