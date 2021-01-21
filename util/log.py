import logging
import sys
import os
import threading

from util.config import global_config
import util.misc
import lib.twilio_wrap
import util.error

log_dir = os.path.abspath(global_config['log']['dir'])
default_log_file_name = global_config['log']['default_log_file']

class NotifyHandler(logging.StreamHandler):
    def __init__(self):
        logging.StreamHandler.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        lib.twilio_wrap.send_sms(msg)

class ThreadFileHandler(logging.Handler):
    def __init__(self, dirname, default_log_file):
        super(ThreadFileHandler, self).__init__()
        self.files = {}
        self.dirname = dirname
        self.default_log_file = default_log_file
        self.default_log_fp = None
        if not os.access(dirname, os.W_OK):
            raise util.error.LocalError("Directory %s not writeable" % dirname)

    def flush(self):
        self.acquire()
        try:
            for fp in self.files.values():
                fp.flush()
        finally:
            self.release()

    def _get_default_log_fp(self):
        if self.default_log_fp is None:
            self.default_log_fp = open(os.path.join(self.dirname, self.default_log_file), "a")
        return self.default_log_fp

    def start_log(self, log_name, thread_name):
        if thread_name not in self.files:
            fp = open(os.path.join(f"{util.misc.get_now_datetime()}-{thread_name}-{log_name}.log"))
            self.files[thread_name] = fp

    def stop_log(self, thread_name):
        if thread_name not in self.files:
            fp = self.files.pop(thread_name)
            fp.close()

    def emit(self, record):
        # No lock here; following code for StreamHandler and FileHandler
        try:
            thread_name = record.threadName or (str(record.thread) if record.thread else None)
            fp = self.files[thread_name] if thread_name in self.files else self._get_default_log_fp()
            msg = self.format(record)
            fp.write('%s\n' % msg.encode("utf-8"))
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self.handleError(record)

    def close(self):
        for k, fp in self.files.items():
            fp.close()
        self.default_log_fp.close()

def get_console_handler(level):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    return console_handler

def get_log_file_name_with_timestamp(log_name):
    if not os.path.isdir(log_dir):
        raise ValueError(f'Can not find log dir {log_dir}')
    log_filename = util.misc.get_now_datetime()+'_'+log_name
    return log_dir + os.sep + log_filename

def get_file_handler(level, log_dir, defaul_log_file):
    file_handler = ThreadFileHandler(log_dir, defaul_log_file)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(level)
    return file_handler

def get_notification_handler(level):
    notify_handler = NotifyHandler()
    notify_handler.setLevel(level)
    return notify_handler


_logger = logging.getLogger()
_logger.setLevel(logging.DEBUG)
_logger.propagate = False

_console_handler = get_console_handler(logging.ERROR)
_notification_handler = get_notification_handler(logging.CRITICAL)
_file_handler = get_file_handler(logging.DEBUG, log_dir, default_log_file_name)

_logger.addHandler(_console_handler)
_logger.addHandler(_notification_handler)
_logger.addHandler(_file_handler)

def start_log(log_name, thread_name=None):
    global _file_handler
    _file_handler.start_log()
    log_path = get_log_file_name_with_timestamp(log_name)
    _file_handler = get_file_handler(logging.DEBUG, log_path)
    _logger.addHandler(_file_handler)
    return log_path

def stop_log():
    global _file_handler
    _logger.addHandler(_file_handler)
