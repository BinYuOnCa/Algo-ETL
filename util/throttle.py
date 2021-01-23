from queue import Queue
import time

# This convenience func preserves name and docstring
from functools import wraps

MINIMUM_WAIT_TIME = 0.2  # 0.2s

def throttle(period, max_times, on_throttle=None):
    '''
    If the calling of function is too frequent, it will sleep for a while
    period - seconds of a monitoring period
    max_times - maximum times that it can run
    on_throttle - params is (sleep_time, run_queue). sleep_time is in second
    '''
    def decorator(func):
        run_q = Queue(maxsize=max_times)

        @wraps(func)
        def wrapper(*args, **kwargs):
            now = time.time()
            while not run_q.empty and run_q.queue[0] < now - period:
                run_q.get()
            if run_q.full():
                sleep_time = max(period - (now - run_q.queue[0]), MINIMUM_WAIT_TIME)
                if sleep_time > 0:
                    if on_throttle:
                        on_throttle(sleep_time, run_q)
                    time.sleep(sleep_time)
                run_q.get()
            run_q.put(now)
            return func(*args, **kwargs)

        return wrapper
    return decorator
