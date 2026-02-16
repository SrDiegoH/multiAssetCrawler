import re
import requests

from log.log_manager import log_debug

def get_substring(text, start_text, end_text, replace_by_paterns=[], should_remove_tags=False):
    start_index = text.find(start_text)
    new_text = text[start_index:]

    end_index = new_text[len(start_text):].find(end_text) + len(start_text)
    cutted_text = new_text[len(start_text):end_index]

    if not cutted_text:
        return None

    clean_text = cutted_text.replace('\n', '').replace('\t', '')

    no_tags_text = re.sub(r'<[^>]*>', '', clean_text) if should_remove_tags else clean_text

    final_text = no_tags_text
    for pattern in replace_by_paterns:
        final_text = final_text.replace(pattern, '')

    return final_text.strip()

def text_to_number(text, should_convert_thousand_decimal_separators=True, convert_percent_to_decimal=False):
    try:
        if not text:
            raise Exception()

        if not isinstance(text, str):
            return text

        text = text.strip()

        if not text.strip():
            raise Exception()

        if should_convert_thousand_decimal_separators:
            text = text.replace('.','').replace(',','.')
        else:
            text = text.replace(',','')

        if '%' in text:
            return float(text.replace('%', '').strip()) / (100 if convert_percent_to_decimal else 1)

        if 'R$' in text:
            text = text.replace('R$', '')
        elif 'US$' in text:
            text = text.replace('US$', '')
        elif '$' in text:
            text = text.replace('$', '')

        return float(text.strip())
    except:
        return 0

def multiply_by_unit(data, should_convert_thousand_decimal_separators=True, convert_percent_to_decimal=False):
    if not data:
        return None

    if 'K' in data:
        return text_to_number(data.replace('K', ''), should_convert_thousand_decimal_separators=should_convert_thousand_decimal_separators, convert_percent_to_decimal=convert_percent_to_decimal) * 1_000
    elif 'M' in data:
        return text_to_number(data.replace('Milhões', '').replace('Millions', '').replace('M', ''), should_convert_thousand_decimal_separators=should_convert_thousand_decimal_separators, convert_percent_to_decimal=convert_percent_to_decimal) * 1_000_000
    elif 'B' in data:
        return text_to_number(data.replace('Bilhões', '').replace('Billions', '').replace('B', ''), should_convert_thousand_decimal_separators=should_convert_thousand_decimal_separators, convert_percent_to_decimal=convert_percent_to_decimal) * 1_000_000_000
    elif 'T' in data:
        return text_to_number(data.replace('Trilhões', '').replace('Trillions', '').replace('T', ''), should_convert_thousand_decimal_separators=should_convert_thousand_decimal_separators, convert_percent_to_decimal=convert_percent_to_decimal) * 1_000_000_000_000

    return text_to_number(data, should_convert_thousand_decimal_separators=should_convert_thousand_decimal_separators, convert_percent_to_decimal=convert_percent_to_decimal)

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
    if first_dict and second_dict:
        combined_dict = {**first_dict, **second_dict}
        log_debug(f'Data from combined Frist and Second Dictionaries: {combined_dict}')
    elif first_dict:
        combined_dict = first_dict
        log_debug(f'Data from First Dictionary only: {combined_dict}')
    elif second_dict:
        combined_dict = second_dict
        log_debug(f'Data from Second Dictionary only: {combined_dict}')
    else:
        combined_dict = {}
        log_debug('No combined data')

    missing_combined_infos = filter_remaining_infos(combined_dict, info_names)
    log_debug(f'Missing info from Combined data: {missing_combined_infos}')
    return combined_dict, missing_combined_infos

def get_parameter_info(params, name, default=None):
    return params.get(name, default).replace(' ', '').lower()

def get_cache_parameter_info(params, name, default='0'):
    return get_parameter_info(params, name, default) in { '1', 's', 'sim', 't', 'true', 'y', 'yes' }