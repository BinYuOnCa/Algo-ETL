import functools
import time
import logging

def on_error(default_handler=None):  # default is to do nothing
    def decorator(func):
        @functools.wraps(func)  # keep func info
        def wrapper(*args, **kwargs):
            def re_run():
                return func(*args, **kwargs)

            error_handler = kwargs.pop('on_error') if 'on_error' in kwargs else default_handler
            if callable(error_handler):
                try:
                    ret = func(*args, **kwargs)
                except Exception as e:
                    return error_handler(e, re_run=re_run)
                else:  # no exception
                    return ret
            else:
                return func(*args, **kwargs)
        return wrapper
    return decorator

def error_handler_coerce(e, re_run):
    logging.exception(e)
    return None

def error_handler_raise(e, re_run):
    raise e

def error_handler_wait_and_rerun(wait_time, re_run_times=1):
    def error_handler(e, re_run):
        for i in range(re_run_times):
            time.sleep(wait_time)
            try:
                ret = re_run()
            except Exception as e_re_run:
                logging.exception(e_re_run)
                continue
            return ret
        raise e
    return error_handler






# # Wrapping exceptions
# class MylibError(Exception):
#     """Generic exception for mylib"""
#     def __init__(self, msg, original_exception):
#         super(MylibError, self).__init__(msg + (": %s" % original_exception))
#         self.original_exception = original_exception

# try:
#     requests.get("http://example.com")
# except requests.exceptions.ConnectionError as e:
#      raise MylibError("Unable to connect", e)