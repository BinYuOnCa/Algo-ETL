import time
from utils.stock_download_import import Etl

"""
This is used to download daily data and import to database
Use API function
"""
begin_time = int(time.time())
item = "day"
dt = Etl(item)
dt.stock_download_import()
end_time = int(time.time())
print(f"This task spend  {round((end_time-begin_time)/60,2)} minutes.")
