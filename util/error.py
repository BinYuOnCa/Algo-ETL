# This convenience func preserves name and docstring
from functools import wraps
import logging
import time

class BaseError(Exception):
    '''Root of My Error'''
    pass

class LocalError(BaseError):
    '''Internal error'''
    pass

class RemoteError(BaseError):
    '''Remote Error includes network erros and remote host errors'''
    pass

class NetworkError(RemoteError):
    '''LAN or WAN error'''
    pass

class RemoteHostError(RemoteError):
    '''remote host error'''
    pass

class DecodeError(RemoteHostError):
    '''Cannot decode correctly the data from remote host'''
    pass

class FormatError(RemoteHostError):
    '''The data from remote host is decoded but with wrong format'''
    pass

class NoDataError(RemoteHostError):
    '''Cannot find useful data'''
    pass


def on_error(handler=None):  # default is to do nothing
    def decorator(func):
        @wraps(func)  # keep func info
        def wrapper(*args, **kwargs):
            def re_run():
                return func(*args, **kwargs)

            # handler in calling param 'on_error' overides the default decorater handler
            error_handler = kwargs.pop('on_error') if 'on_error' in kwargs else handler
            try:
                ret = func(*args, **kwargs)
            except Exception as e:
                if error_handler:
                    error_handler(e, re_run=re_run)
                else:  # handler is None
                    logging.exception(e)
            else:  # no exception
                return ret

        return wrapper
    return decorator

def error_handler_coerce(e, re_run):
    logging.exception(e)
    return None

def error_handler_raise(e, re_run):
    raise e

def get_error_handler(wait_time=0, re_run_times=0, exceptions_handled=None, raise_if_same_errors: int = 1):
    '''
    Create a error handler

    '''
    if not exceptions_handled:
        exceptions_tuple = (Exception,)
    elif issubclass(exceptions_handled, Exception):
        exceptions_tuple = (exceptions_handled,)
    elif type(exceptions_handled) is list:
        exceptions_tuple = tuple(exceptions_handled)
    else:
        exceptions_tuple = (Exception,)

    stop_counter = 1  # counter of same error happened in a row
    last_exception_type = None

    def error_handler(e, re_run):
        # re-run handling
        if re_run_times > 0:
            logging.exception(e)  # TODO add re-run time to log
            for i in range(re_run_times):
                if wait_time > 0:
                    time.sleep(wait_time)
                try:
                    ret = re_run()
                except exceptions_tuple as e_re_run:
                    logging.exception(e_re_run)
                    logging.error(f'Fail to retry {i} times')
                    continue
                return ret

        # raise only when same errors happened
        nonlocal stop_counter
        nonlocal last_exception_type
        if type(e) == last_exception_type:
            stop_counter += 1
        if stop_counter >= raise_if_same_errors:
            stop_counter = 1
            last_exception_type = None
            raise e
        else:
            logging.exception(e)  # TODO add reason to coerce the excpetion

    return error_handler
