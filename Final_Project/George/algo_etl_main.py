import threading
from datetime import datetime

import algo_etl


def start_thread(candle_freq):
    print(f"start processing for candle frequency: ", candle_freq, "at time: ", datetime.now())
    algo_etl.algo_etl_main_control(candle_freq)
    print(f"candle frequency: ", candle_freq, "process finished at time: ",datetime.now())

if __name__ == "__main__":
   # for candle_freq in [["D"], [1]]:
   #     t = threading.Thread(target=start_thread, args=(candle_freq,))
   #     t.start()
   #     t.join()
   t_1 = threading.Thread(target=start_thread, args=(["D"],))
   t_2 = threading.Thread(target=start_thread, args=([1],))

   t_1.start()
   t_2.start()

   t_1.join()
   t_2.join()