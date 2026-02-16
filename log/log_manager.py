from datetime import datetime
import os

DATE_FORMAT = '%d-%m-%Y %H:%M:%S'

DEBUG_LOG_LEVEL = 'DEBUG'
ERROR_LOG_LEVEL = 'ERROR'
INFO_LOG_LEVEL = 'INFO'

LOG_LEVEL = os.environ.get('LOG_LEVEL', ERROR_LOG_LEVEL)

def log_error(message):
    if LOG_LEVEL == ERROR_LOG_LEVEL or LOG_LEVEL == INFO_LOG_LEVEL or LOG_LEVEL == DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(DATE_FORMAT)} - {ERROR_LOG_LEVEL} - {message}')

def log_info(message):
    if LOG_LEVEL == INFO_LOG_LEVEL or LOG_LEVEL == DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(DATE_FORMAT)} - {INFO_LOG_LEVEL} - {message}')

def log_debug(message):
    if LOG_LEVEL == DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(DATE_FORMAT)} - {DEBUG_LOG_LEVEL} - {message}')
