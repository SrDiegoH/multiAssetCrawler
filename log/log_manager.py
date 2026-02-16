from datetime import datetime
import os

_DATE_FORMAT = '%d-%m-%Y %H:%M:%S'

_DEBUG_LOG_LEVEL = 'DEBUG'
_ERROR_LOG_LEVEL = 'ERROR'
_INFO_LOG_LEVEL = 'INFO'

LOG_LEVEL = os.environ.get('LOG_LEVEL', _ERROR_LOG_LEVEL)

def log_error(message):
    if LOG_LEVEL == _ERROR_LOG_LEVEL or LOG_LEVEL == _INFO_LOG_LEVEL or LOG_LEVEL == _DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(_DATE_FORMAT)} - {_ERROR_LOG_LEVEL} - {message}')

def log_info(message):
    if LOG_LEVEL == _INFO_LOG_LEVEL or LOG_LEVEL == _DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(_DATE_FORMAT)} - {_INFO_LOG_LEVEL} - {message}')

def log_debug(message):
    if LOG_LEVEL == _DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(_DATE_FORMAT)} - {_DEBUG_LOG_LEVEL} - {message}')