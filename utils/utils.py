import re
import requests

from log.log_manager import log_debug

_ASSET_TYPES = [ 'REIT', 'STOCK', 'ETF' ]

_TAG_REGEX = re.compile(r'<[^>]*>')

_UNIT_MULTIPLIERS = {
    'k': 1_000,
    'm': 1_000_000,
    'b': 1_000_000_000,
    't': 1_000_000_000_000,
}

_UNIT_WORDS = {
    'milhões': 'm',
    'millions': 'm',
    'bilhões': 'b',
    'billions': 'b',
    'trilhões': 't',
    'trillions': 't',
}

"""
<DEPRECATED> - Testing new function before delete this code
def get_substring(text, start_text, end_text, replace_by_patterns=[], should_remove_tags=False):
    start_index = text.find(start_text)
    new_text = text[start_index:]

    end_index = new_text[len(start_text):].find(end_text) + len(start_text)
    extracted = new_text[len(start_text):end_index]

    if not extracted:
        return None

    clean_text = extracted.replace('\n', '').replace('\t', '')

    no_tags_text = re.sub(r'<[^>]*>', '', clean_text) if should_remove_tags else clean_text

    for pattern in replace_by_patterns:
        final_text = no_tags_text.replace(pattern, '')

    return final_text.strip()
"""

def get_substring(text, start_text, end_text, replace_by_patterns=None, should_remove_tags=False):
    if not text or start_text not in text:
        return None

    replace_by_patterns = replace_by_patterns or []

    try:
        extracted = text.split(start_text, 1)[1].split(end_text, 1)[0]
    except IndexError:
        return None

    clean_text = extracted.replace('\n', '').replace('\t', '').strip()

    no_tags_text = _TAG_REGEX.sub('', clean_text) if should_remove_tags else clean_text

    for pattern in replace_by_patterns:
        no_tags_text = no_tags_text.replace(pattern, '')

    return no_tags_text.strip() or None

def text_to_number(text, should_convert_thousand_decimal_separators=True, convert_percent_to_decimal=False):
    if text is None:
        return 0

    if not isinstance(text, str):
        return text

    text = text.strip().lower()
    if not text:
        raise Exception()

    for currency in ('r$', 'us$', '$'):
        text = text.replace(currency, '')

    is_percent = '%' in text
    text = text.replace('%', '').strip() if is_percent else text

    text = text.replace('.', '').replace(',', '.') if should_convert_thousand_decimal_separators else text.replace(',', '')

    try:
        value = float(text)
    except ValueError:
        return 0

    if is_percent and convert_percent_to_decimal:
        value /= 100

    return value

def multiply_by_unit(data, should_convert_thousand_decimal_separators=True, convert_percent_to_decimal=False):
    if not data:
        return None

    if not isinstance(data, str):
        return data

    text = data.strip().lower()

    for word, letter in _UNIT_WORDS.items():
        if word in text:
            text = text.replace(word, letter)

    unit = text[-1]

    if unit in _UNIT_MULTIPLIERS.keys():
        number_part = text[:-1].strip()
        value = text_to_number(number_part, should_convert_thousand_decimal_separators=should_convert_thousand_decimal_separators, convert_percent_to_decimal=convert_percent_to_decimal)
        return value * _UNIT_MULTIPLIERS[unit]

    return text_to_number(text, should_convert_thousand_decimal_separators=should_convert_thousand_decimal_separators, convert_percent_to_decimal=convert_percent_to_decimal)

def request_get(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    log_debug(f'Response from {url} : {response}')

    return response

def filter_remaining_infos(data, info_names, default_info_names=None):
    if not data:
        return info_names

    missing_info = [ info for info in info_names if info in data and data[info] is None ]

    return missing_info if missing_info else default_info_names

def combine_data(first_dict, second_dict, info_names):
    first_dict = first_dict or {}
    second_dict = second_dict or {}
    combined_dict = { **first_dict, **second_dict }

    if first_dict and second_dict:
        log_debug(f'Data from combined First and Second Dictionaries: {combined_dict}')
    elif first_dict:
        log_debug(f'Data from First Dictionary only: {combined_dict}')
    elif second_dict:
        log_debug(f'Data from Second Dictionary only: {combined_dict}')
    else:
        log_debug('No combined data')

    missing_combined_infos = filter_remaining_infos(combined_dict, info_names)
    log_debug(f'Missing info from Combined data: {missing_combined_infos}')

    return combined_dict, missing_combined_infos

def get_parameter_info(params, name, default=None):
    value = params.get(name, default)
    return None if value is None else str(value).strip().lower()

def get_cache_parameter_info(params, name, default='0'):
    return get_parameter_info(params, name, default) in { '1', 's', 'sim', 't', 'true', 'v', 'verdade', 'verdadeiro', 'y', 'yes' }

def remove_type_from_name(text):
    return re.sub(r'\b(' + '|'.join(_ASSET_TYPES) + r')\b', '', text.upper()).strip()