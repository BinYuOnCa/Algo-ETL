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

def get_error_handler(wait_time=0, re_run_times=0, exceptions_handled=None):
    '''
    Generate error_handle function for on_error decorator

    Args:
        wait_time (int, optional): wait in second. Defaults to 0.
        re_run_times (int, optional): Defaults to 0.
        exceptions_handled (Union(Exception, [Excption]), optional): Defaults to None.

    Raises:
        e: [description]

    Returns:
        [type]: [description]
    '''
    if not exceptions_handled:
        exceptions_tuple = (Exception,)
    elif issubclass(exceptions_handled, Exception):
        exceptions_tuple = (exceptions_handled,)
    elif type(exceptions_handled) is list:
        exceptions_tuple = tuple(exceptions_handled)
    else:
        exceptions_tuple = (Exception,)

    def error_handler(e, re_run):
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
        raise e
    return error_handler
