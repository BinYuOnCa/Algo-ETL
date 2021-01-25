import logging
from logging.handlers import TimedRotatingFileHandler
import utils.param as param


def get_log_level(log_level=None):
    """
    get log level from param config
    :param: log_txt (str)
    :param: level (str) , optional
    :return:
    """
    if log_level is None:
        return logging.INFO
    elif log_level == 'DEBUG':
        return logging.DEBUG
    elif log_level == 'INFO':
        return logging.INFO
    elif log_level == 'WARNING':
        return logging.WARNING
    elif log_level == 'ERROR':
        return logging.ERROR
    elif log_level == 'CRITICAL':
        return logging.CRITICAL


def init_log():
    """
    Initialize logger
    :param:
    :return:
    """
    global logger
    logger = logging.getLogger()
    logger.setLevel(get_log_level(param.DEFAULT_LOG_LEVEL))
    formatter = logging.Formatter('%(asctime)s - '
                                  '%(name)s - '
                                  '%(levelname)s - '
                                  '%(message)s')
    logfile = param.LOG_FILE_NAME
    handler = TimedRotatingFileHandler(logfile,
                                       when="D",
                                       interval=1,
                                       backupCount=10)
    handler.setFormatter(formatter)
    handler.suffix = "%Y%m%d_%H%M%S.log"
    logger.addHandler(handler)


def output_log(log_txt, level=None):
    """
    write log
    :param: log_txt (str)
    :param: level (str) , optional
    :return:
    """
    if level is None:
        logger.info(log_txt)
    elif level == param.LOG_LEVEL_DEBUG:
        logger.debug(log_txt)
    elif level == param.LOG_LEVEL_INFO:
        logger.info(log_txt)
    elif level == param.LOG_LEVEL_WARNING:
        logger.warning(log_txt)
    elif level == param.LOG_LEVEL_ERROR:
        logger.error(log_txt, exc_info=True)
    elif level == param.LOG_LEVEL_CRITICAL:
        logger.critical(log_txt)
