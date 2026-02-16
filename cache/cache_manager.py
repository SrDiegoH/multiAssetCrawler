import ast
from datetime import datetime, timedelta
import os

CACHE_FILE_FII = '/tmp/fii_cache.txt'
CACHE_FILE_ACAO = '/tmp/acao_cache.txt'
CACHE_FILE_STOCK = '/tmp/stock_cache.txt'
CACHE_FILE_REIT = '/tmp/reit_cache.txt'
CACHE_FILE_ETF = '/tmp/etf_cache.txt'
CACHE_FILE_CRIPTO = '/tmp/cripto_cache.txt'

CACHE_EXPIRY = timedelta(days=1)

DATE_FORMAT = '%d-%m-%Y %H:%M:%S'

def cache_exists(file):
    if os.path.exists(file):
        return True

    log_info('No cache file found')
    return False

def upsert_cache(id, data, file):
    lines = []
    updated = False

    if cache_exists(file):
        with open(file, 'r') as cache_file:
            lines = cache_file.readlines()

    with open(file, 'w') as cache_file:
        for line in lines:
            if not line.startswith(id):
                cache_file.write(line)
                continue

            _, old_cached_date_as_text, old_data_as_text = line.strip().split(SEPARATOR)
            old_data = ast.literal_eval(old_data_as_text)

            combined_data = { **old_data, **data }
            updated_line = f'{id}{SEPARATOR}{old_cached_date_as_text}{SEPARATOR}{combined_data}\n'
            cache_file.write(updated_line)
            updated = True

        if not updated:
            new_line = f'{id}{SEPARATOR}{datetime.now().strftime(DATE_FORMAT)}{SEPARATOR}{data}\n'
            cache_file.write(new_line)
            log_info(f'New cache entry created for "{id}"')

    if updated:
        log_info(f'Cache updated for "{id}"')

def clear_cache(id, file):
    if not cache_exists(file):
        return

    log_debug('Cleaning cache')

    with open(file, 'r') as cache_file:
        lines = cache_file.readlines()

    with open(file, 'w') as cache_file:
        cache_file.writelines(line for line in lines if not line.startswith(id))

    log_info(f'Cache cleaning completed for "{id}"')

def read_cache(id, file):
    if not cache_exists(file):
        return None

    log_debug('Reading cache')

    clear_cache_control = False

    with open(file, 'r') as cache_file:
        for line in cache_file:
            if not line.startswith(id):
                continue

            _, cached_date_as_text, data = line.strip().split(SEPARATOR)
            cached_date = datetime.strptime(cached_date_as_text, DATE_FORMAT)

            if datetime.now() - cached_date <= CACHE_EXPIRY:
                log_debug(f'Cache hit for "{id}" (Date: {cached_date_as_text})')
                return ast.literal_eval(data)

            log_debug(f'Cache expired for "{id}" (Date: {cached_date_as_text})')
            clear_cache_control = True
            break

    if clear_cache_control:
        clear_cache(id, file)

    log_info(f'No cache entry found for "{id}"')
    return None

def delete_cache(file):
    if not cache_exists(file):
        return

    log_debug('Deleting cache')

    os.remove(file)

    log_info('Cache deletion completed')

def preprocess_cache(id, file, should_delete_all_cache, should_clear_cached_data, should_use_cache):
    if should_delete_all_cache:
        delete_cache(file)
    elif should_clear_cached_data:
        clear_cache(id, file)

    can_use_cache = should_use_cache and not (should_delete_all_cache or should_clear_cached_data)

    return can_use_cache
