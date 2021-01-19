import logging
import sys
import os

from util.config import global_config
import lib.twilio_wrap

log_dir = os.path.abspath(global_config['log']['dir'])
default_log_file_name = log_dir + os.sep + 'default_log'

class NotifyHandler(logging.StreamHandler):
    def __init__(self):
        logging.StreamHandler.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        util.notification.post_msg_to_slack(msg, raise_error=False)


def get_console_handler(level):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    return console_handler

def get_log_file_name_with_timestamp(log_name):
    if not os.path.isdir(log_dir):
        raise ValueError(f'Can not find log dir {log_dir}')
    log_filename = util.misc.get_now_datetime()+'_'+log_name
    return log_dir + os.sep + log_filename

def get_file_handler(level, log_file_name):
    file_handler = TimedRotatingFileHandler(log_file_name)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(level)
    return file_handler

def get_notification_handler(level):
    notify_handler = NotififyHandler()
    notify_handler.setLevel(level)
    return notify_handler

def _delete_handler(handler):
    if handler in _logger.handlers:
        _logger.removeHandler(handler)

_logger_name = 'mykb'
_logger = logging.getLogger(_logger_name)
_logger.setLevel(logging.DEBUG)
_logger.propagate = False

_console_handler = get_console_handler(logging.ERROR)
_notification_handler = get_notification_handler(logging.CRITICAL)
_default_file_handler = get_file_handler(logging.DEBUG, default_log_file_name)
_file_handler = _default_file_handler

_logger.addHandler(_console_handler)
_logger.addHandler(_notification_handler)
_logger.addHandler(_file_handler)

def get_logger():
    return _logger

def start_log(log_name):
    global _file_handler
    _delete_handler(_file_handler)
    log_path = get_log_file_name_with_timestamp(log_name)
    _file_handler = get_file_handler(logging.DEBUG, log_path)
    _logger.addHandler(_file_handler)
    return log_path

def stop_log():
    global _file_handler
    _delete_handler(_file_handler)
    _file_handler = _default_file_handler
    _logger.addHandler(_file_handler)
    return