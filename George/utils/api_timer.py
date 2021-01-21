import time

import config.config_parser as conf

class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""

class api_timer:
    """a class to handle the api_rate limit"""
    def __init__(self, limit=conf.settings()["api_limit"], limit_time=conf.settings()["api_limit_time"]):
        self._start_time = None
        self._limit = int(limit)
        self._limit_time = int(limit_time)
        self._call_counter = 0
        self._elapsed_time = 0
        self.start()

    def start(self):
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = time.perf_counter()

    def stop(self):
        """Stop the timer"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        # elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None
        # print(f"Elapsed time: {elapsed_time:0.4f} seconds")

    def reset(self):
        """reset the timer"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        # elapsed_time = time.perf_counter() - self._start_time
        self._start_time = time.perf_counter()
        # print(f"Elapsed time: {elapsed_time:0.4f} seconds")

    def elapsed(self):
        """report the elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        elapsed_time = time.perf_counter() - self._start_time

        # print(f"Elapsed time: {elapsed_time:0.4f} seconds")
        return elapsed_time.__round__(2)

    def isstop(self):
        """test if already stopped"""
        return self._start_time is None

    def isstart(self):
        "test if already started"
        return self._start_time is not None

    def api_timer_handler(self):
        """keep track and handles the api sleep"""
        self._call_counter += 1
        if self.isstop():
            time.sleep(self._limit_time)
            self._call_counter = 0
            self._elapsed_time = 0
            self.start()
            pass
        self._elapsed_time = self.elapsed()
        if self._call_counter >= self._limit - 1:
            if self._elapsed_time <= self._limit_time + 2:
                # need sleep
                # print("sleep " + str(self._limit_time - self._elapsed_time))
                time.sleep(abs(self._limit_time - self._elapsed_time) + 2)
                self._call_counter = 0
                self._elapsed_time = 0
            else:
                # reset timer and counter
                if self.isstop():
                    self.start()
                else:
                    self.reset()
                self._call_counter = 0
                self._elapsed_time = 0
        else:
            if self._elapsed_time <= self._limit_time:
                pass
            else:
                # reset timer
                if self.isstop():
                    self.start()
                else:
                    self.reset()
                self._call_counter = 0
                self._elapsed_time = 0

