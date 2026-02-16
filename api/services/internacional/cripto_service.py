from datetime import datetime, timedelta
import json
import traceback

from cache.cache_manager import CACHE_FILE_CRIPTO, get_data_from_cache
from log.log_manager import log_debug, log_error, log_info
from utils.utils import (
    combine_data,
    filter_remaining_infos,
    get_substring,
    request_get,
    text_to_number,
)

VALID_CRIPTO_SOURCES = {
    'ALL_SOURCE': 'all',
    'BINANCE_SOURCE': 'binance',
    'INVESTIDOR10_SOURCE': 'investidor10',
    'COINMARKETCAP_SOURCE': 'coinmarketcap'
}

VALID_CRIPTO_INFOS = [
    'avg_price',
    'dy',
    'initial_date',
    'latest_dividend',
    'latests_dividends',
    'link',
    'liquidity',
    'market_value',
    'max_52_weeks',
    'mayer_multiple',
    'min_52_weeks',
    'name',
    'price',
    'sector',
    'total_issued_shares',
    'variation_12m',
    'variation_30d'
]

def _convert_binance_cripto_data(earn_apr_data, earn_amount_data, info_names):
    latests_dividends = min(earn_amount_data['data']['estimatedEarningsForm'], key=lambda earn_estimation: earn_estimation['duration'])['amountList'][0]

    ALL_INFO = {
        'avg_price': lambda: None,
        'dy': lambda: text_to_number(earn_apr_data['data']['savingFlexibleProduct'][0]['apy'], should_convert_thousand_decimal_separators=False) * 100,
        'initial_date': lambda: None,
        'latest_dividend': lambda: text_to_number(f'{(((1 + latests_dividends) ** (1 / 52.1428652)) -1):.8f}', should_convert_thousand_decimal_separators=False),
        'latests_dividends': lambda: text_to_number(f'{latests_dividends:.8f}', should_convert_thousand_decimal_separators=False),
        'link': lambda: None,
        'liquidity': lambda: None,
        'market_value': lambda: None,
        'max_52_weeks': lambda: None,
        'mayer_multiple': lambda: None,
        'min_52_weeks': lambda: None,
        'name': lambda: None,
        'price': lambda: None,
        'sector': lambda: None,
        'total_issued_shares': lambda: None,
        'variation_12m': lambda: None,
        'variation_30d': lambda: None,
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def _get_data_from_binance(name, code, info_names):
    try:
        headers = {
          'accept': '*/*',
          'accept-language': 'pt-BR,pt;q=0.9,en;q=0.8,ko;q=0.7,es;q=0.6,fr;q=0.5',
          'bnc-currency': 'BRL_USD',
          'bnc-level': '0',
          'bnc-time-zone': 'America/Sao_Paulo',
          'bnc-uuid': 'b29dbbb4-3c72-4ddd-a405-50faa5d6798b',
          'cache-control': 'no-cache',
          'clienttype': 'web',
          'content-type': 'application/json',
          'csrftoken': 'd41d8cd98f00b204e9800998ecf8427e',
          'device-info': 'eyJzY3JlZW5fcmVzb2x1dGlvbiI6IjE5MjAsMTA4MCIsImF2YWlsYWJsZV9zY3JlZW5fcmVzb2x1dGlvbiI6IjE5MjAsMTAzMiIsInN5c3RlbV92ZXJzaW9uIjoiV2luZG93cyAxMCIsImJyYW5kX21vZGVsIjoidW5rbm93biIsInN5c3RlbV9sYW5nIjoicHQtQlIiLCJ0aW1lem9uZSI6IkdNVC0wMzowMCIsInRpbWV6b25lT2Zmc2V0IjoxODAsInVzZXJfYWdlbnQiOiJNb3ppbGxhLzUuMCAoV2luZG93cyBOVCAxMC4wOyBXaW42NDsgeDY0KSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvMTQzLjAuMC4wIFNhZmFyaS81MzcuMzYgT1BSLzEyNy4wLjAuMCIsImxpc3RfcGx1Z2luIjoiUERGIFZpZXdlcixDaHJvbWUgUERGIFZpZXdlcixDaHJvbWl1bSBQREYgVmlld2VyLE1pY3Jvc29mdCBFZGdlIFBERiBWaWV3ZXIsV2ViS2l0IGJ1aWx0LWluIFBERiIsImNhbnZhc19jb2RlIjoiMWQ1MmNjOGEiLCJ3ZWJnbF92ZW5kb3IiOiJHb29nbGUgSW5jLiAoQU1EKSIsIndlYmdsX3JlbmRlcmVyIjoiQU5HTEUgKEFNRCwgQU1EIFJhZGVvbihUTSkgVmVnYSA4IEdyYXBoaWNzICgweDAwMDAxNUQ4KSBEaXJlY3QzRDExIHZzXzVfMCBwc181XzAsIEQzRDExKSIsImF1ZGlvIjoiMTI0LjA0MzQ3NTI3NTE2MDc0IiwicGxhdGZvcm0iOiJXaW4zMiIsIndlYl90aW1lem9uZSI6IkFtZXJpY2EvU2FvX1BhdWxvIiwiZGV2aWNlX25hbWUiOiJDaHJvbWUgVjE0My4wLjAuMCAoV2luZG93cykiLCJmaW5nZXJwcmludCI6ImY4NjMyMWYyNDlkOWJjMjY0ZDRjYzM1NDA5NDU2M2IzIiwiZGV2aWNlX2lkIjoiIiwicmVsYXRlZF9kZXZpY2VfaWRzIjoiIn0=',
          'dnt': '1',
          'fvideo-id': '3337a37a865da35992d93914dd7f69e20ce82f16',
          'fvideo-token': '7EgI88p6K30eSNITXdPyiQYj754JokYCnAmSCmH+ATWhD3M/2LUe76mNjxMlTwyJk9WLxL+p6H51Z1bi7u/SrOaNRa11cpMibmA3dfjLl0DlKYApkNuX1ece4XEtmYSI86dDt5IdZvjSysBWg2acXCitbCXWjWyORDaUDDBq3eGj7jEGgTTZ+Cj0Mb8vSJofY=47',
          'lang': 'en-BH',
          'pragma': 'no-cache',
          'priority': 'u=1, i',
          'referer': 'https://www.binance.com/en-BH/earn/apr-calculator',
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 OPR/127.0.0.0',
          'x-trace-id': '4f4e5166-6779-487c-a0c5-baaf878ef23b',
          'x-ui-request-trace': '4f4e5166-6779-487c-a0c5-baaf878ef23b'
        }

        response = request_get(f'https://www.binance.com/bapi/earn/v3/friendly/finance-earn/calculator/product/list?asset={code}&type=Flexible', headers)
        earn_apr_data = response.json()

        headers = {
            'accept': '*/*',
            'accept-language': 'pt-BR,pt;q=0.9,en;q=0.8,ko;q=0.7,es;q=0.6,fr;q=0.5',
            'bnc-currency': 'BRL_USD',
            'bnc-level': '0',
            'bnc-time-zone': 'America/Sao_Paulo',
            'bnc-uuid': 'b29dbbb4-3c72-4ddd-a405-50faa5d6798b',
            'cache-control': 'no-cache',
            'clienttype': 'web',
            'content-type': 'application/json',
            'csrftoken': 'd41d8cd98f00b204e9800998ecf8427e',
            'device-info': 'eyJzY3JlZW5fcmVzb2x1dGlvbiI6IjE5MjAsMTA4MCIsImF2YWlsYWJsZV9zY3JlZW5fcmVzb2x1dGlvbiI6IjE5MjAsMTAzMiIsInN5c3RlbV92ZXJzaW9uIjoiV2luZG93cyAxMCIsImJyYW5kX21vZGVsIjoidW5rbm93biIsInN5c3RlbV9sYW5nIjoicHQtQlIiLCJ0aW1lem9uZSI6IkdNVC0wMzowMCIsInRpbWV6b25lT2Zmc2V0IjoxODAsInVzZXJfYWdlbnQiOiJNb3ppbGxhLzUuMCAoV2luZG93cyBOVCAxMC4wOyBXaW42NDsgeDY0KSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvMTQzLjAuMC4wIFNhZmFyaS81MzcuMzYgT1BSLzEyNy4wLjAuMCIsImxpc3RfcGx1Z2luIjoiUERGIFZpZXdlcixDaHJvbWUgUERGIFZpZXdlcixDaHJvbWl1bSBQREYgVmlld2VyLE1pY3Jvc29mdCBFZGdlIFBERiBWaWV3ZXIsV2ViS2l0IGJ1aWx0LWluIFBERiIsImNhbnZhc19jb2RlIjoiMWQ1MmNjOGEiLCJ3ZWJnbF92ZW5kb3IiOiJHb29nbGUgSW5jLiAoQU1EKSIsIndlYmdsX3JlbmRlcmVyIjoiQU5HTEUgKEFNRCwgQU1EIFJhZGVvbihUTSkgVmVnYSA4IEdyYXBoaWNzICgweDAwMDAxNUQ4KSBEaXJlY3QzRDExIHZzXzVfMCBwc181XzAsIEQzRDExKSIsImF1ZGlvIjoiMTI0LjA0MzQ3NTI3NTE2MDc0IiwicGxhdGZvcm0iOiJXaW4zMiIsIndlYl90aW1lem9uZSI6IkFtZXJpY2EvU2FvX1BhdWxvIiwiZGV2aWNlX25hbWUiOiJDaHJvbWUgVjE0My4wLjAuMCAoV2luZG93cykiLCJmaW5nZXJwcmludCI6ImY4NjMyMWYyNDlkOWJjMjY0ZDRjYzM1NDA5NDU2M2IzIiwiZGV2aWNlX2lkIjoiIiwicmVsYXRlZF9kZXZpY2VfaWRzIjoiIn0=',
            'dnt': '1',
            'fvideo-id': '3337a37a865da35992d93914dd7f69e20ce82f16',
            'fvideo-token': 'PKdntxiBwpNEqV7P81oOZ7tWlqz8LT1XOoRD6tvGryR5z13YmqJuBPhphhuAe2qXTXn/y4UtMHoK7uSllDiUULGFe/zeFLortaRFVw4q3UULvzVKSjLsCCZoUT1QU9nPn4KwgF45meWKePCT1Ovm4LuIZDynB1BQs0BxEx7wsGI0VnVgxT7oYc/u13P35xKAI=5e',
            'lang': 'en-BH',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.binance.com/en-BH/earn/apr-calculator',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 OPR/127.0.0.0',
            'x-trace-id': '8525ae27-e673-4b13-b81d-ec0e318a662b',
            'x-ui-request-trace': '8525ae27-e673-4b13-b81d-ec0e318a662b'
        }

        response = request_get(f'https://www.binance.com/bapi/earn/v2/friendly/finance-earn/calculator/calculate?productId={code}001&amount=1&productType=LENDING_FLEXIBLE&autoTransfer=true', headers)
        earn_amount_data = response.json()

        converted_data = _convert_binance_cripto_data(earn_apr_data, earn_amount_data, info_names)
        log_debug(f'Converted fresh Binance data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data from Binance for "{name}": {traceback.format_exc()}')
        return None

def _convert_investidor10_cripto_data(html_page, json_historical_data, info_names):
    patterns_to_remove = [
        '</div>',
        '<div>',
        '<div class="value">',
        '<div class="_card-body">',
        '</span>',
        '<span>',
        '<span class="value">'
    ]

    last_quote = max(json_historical_data, key=lambda quote: datetime.strptime(quote["created_at"], "%d/%m/%Y"))
    latest_price = text_to_number(last_quote['brl_price'], should_convert_thousand_decimal_separators=False)
    avg_price = sum(text_to_number(item['brl_price'], should_convert_thousand_decimal_separators=False) for item in json_historical_data) / len(json_historical_data)

    ALL_INFO = {
        'avg_price': lambda: avg_price,
        'dy': lambda: None,
        'initial_date': lambda: None,
        'latest_dividend': lambda: None,
        'latests_dividends': lambda: None,
        'link': lambda: None,
        'liquidity': lambda: None,
        'market_value': lambda: None,
        'max_52_weeks': lambda: max(text_to_number(item['brl_price'], should_convert_thousand_decimal_separators=False) for item in json_historical_data),
        #'max_52_weeks': lambda: text_to_number(get_substring(get_substring(html_page, 'Maior cotação em 1 ano</span>', '</small>'), '<small>(R$', ')'), should_convert_thousand_decimal_separators=False),
        'mayer_multiple': lambda: latest_price / avg_price,
        'min_52_weeks': lambda: min(text_to_number(item['brl_price'], should_convert_thousand_decimal_separators=False) for item in json_historical_data),
        #'min_52_weeks': lambda: text_to_number(get_substring(get_substring(html_page, 'Menor cotação em 1 ano</span>', '</small>'), '<small>(R$', ')'), should_convert_thousand_decimal_separators=False),
        'name': lambda: get_substring(html_page, '<h1>', '</h1>', patterns_to_remove),
        'price': lambda: latest_price,
        #'price': lambda: text_to_number(get_substring(html_page, 'Valor em Reais</span>', '</span>', patterns_to_remove), should_convert_thousand_decimal_separators=False),
        'sector': lambda: get_substring(get_substring(html_page, '<span class="label label-default">', '</h2>'), '<span class="label label-default">', '</span>'),
        'total_issued_shares': lambda: None,
        'variation_12m': lambda: text_to_number(get_substring(html_page, 'VARIAÇÃO (12M)</span>', '</span>', patterns_to_remove), should_convert_thousand_decimal_separators=False),
        'variation_30d': lambda: text_to_number(get_substring(html_page, '>30</div>', '</div>'), should_convert_thousand_decimal_separators=False),
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def _get_data_from_investidor10(name, code, info_names):
    try:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'referer': 'https://investidor10.com.br/criptomoedas/bitcoin',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        }

        response = request_get(f'https://investidor10.com.br/criptomoedas/{name}', headers)
        html_page = response.text

        headers = {
          'accept': '*/*',
          'accept-language': 'pt-BR,pt;q=0.9,en;q=0.8,ko;q=0.7,es;q=0.6,fr;q=0.5',
          'dnt': '1',
          'priority': 'u=1, i',
          'referer': 'https://investidor10.com.br/criptomoedas/usd-coin/',
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 OPR/127.0.0.0',
          'x-csrf-token': 'QGre1EFlQ1BGS24nTcTxKqeAZWABENdw8R816A0N',
          'x-requested-with': 'XMLHttpRequest'
        }

        id = get_substring(html_page, 'cryptoId" value="', '"')
        response = request_get(f'https://investidor10.com.br/api/criptomoedas/cotacoes/{id}/365/dollar', headers)
        json_historical_data = response.json()

        converted_data = _convert_investidor10_cripto_data(html_page, json_historical_data, info_names)
        log_debug(f'Converted fresh Investidor 10 data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data from Investidor 10 for "{name}": {traceback.format_exc()}')
        return None

def _convert_coinmarketcap_cripto_data(html_page, json_historical_data, info_names):
    def get_binance_earn():
        earns = json.loads(get_substring(html_page, '"earnList":', '"upcoming"')[:-1])
        earns_binance_flexi = [ earn for earn in earns if earn['provider']['name'] == 'Binance' and earn['subType'] == 'earn_flexi' ]
        total_earn = sum(earns_binance_flexi[0]['apr'])
        return total_earn

    def get_quotes_info(quotes, info, function):
        return function(quote['quote'][info] for quote in quotes)

    quotes = json_historical_data['data']['quotes']
    total_quotes = len(quotes)

    avg_price = get_quotes_info(quotes, 'close', sum) / total_quotes

    last_quote = max(quotes, key=lambda quote: datetime.fromisoformat(quote['timeClose'].replace('Z', '+00:00')))['quote']
    latest_price = last_quote['close']

    ALL_INFO = {
        'avg_price': lambda: avg_price,
        'dy': lambda: get_binance_earn(),
        'initial_date': lambda: get_substring(html_page, '"dateAdded":"', '"'),
        'latest_dividend': lambda: None,
        'latests_dividends': lambda: None,
        'link': lambda: get_substring(html_page, '"website":[', ']', [ '"' ]),
        #'link': lambda: get_substring(get_substring(html_page, 'Site</div>', '" target=') + '>', 'href="//', '>'),
        'liquidity': lambda: get_quotes_info(quotes, 'volume', sum) / total_quotes,
        #'liquidity': lambda: multiply_by_unit(get_substring(get_substring(html_page, 'Volume (24h)</div>', '</span></div>') + '>', 'R$', '>'), should_convert_thousand_decimal_separators=False),
        'market_value': lambda: last_quote['marketCap'],
        #'market_value': lambda: multiply_by_unit(get_substring(get_substring(html_page, 'Capitalização de Mercado</div>', '</span></div>') + '>', '<span>R$', '>'), should_convert_thousand_decimal_separators=False),
        'max_52_weeks': lambda: get_quotes_info(quotes, 'close', max),
        'mayer_multiple': lambda: latest_price / avg_price,
        'min_52_weeks': lambda: get_quotes_info(quotes, 'close', min),
        #'name': lambda: name,
        'name': lambda: get_substring(html_page, '"slug":"', '"'),
        'price': lambda: latest_price,
        #'price': lambda: multiply_by_unit(get_substring(html_page, 'price-display">R$', '<'), should_convert_thousand_decimal_separators=False),
        'sector': lambda: get_substring(html_page, '"category":"', '"'),
        'total_issued_shares': lambda: text_to_number(get_substring(get_substring(html_page, '"totalSupply":{', '},'), '"value":', ','), should_convert_thousand_decimal_separators=False),
        #'total_issued_shares': lambda: multiply_by_unit(get_substring(get_substring(html_page, 'Fornecimento total</div>', 'Fornecimento máximo</div>', [ code ]), 'popover-base"><span>', '</span>'), should_convert_thousand_decimal_separators=False),
        'variation_12m': lambda: text_to_number(get_substring(html_page, '"priceChangePercentage30d":', ','), should_convert_thousand_decimal_separators=False),
        'variation_30d': lambda: text_to_number(get_substring(html_page, '"priceChangePercentage1y":', ','), should_convert_thousand_decimal_separators=False),
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def _get_data_from_coinmarketcap(name, code, info_names):
    try:
        headers = {
          'accept': '*/*',
          'accept-language': 'pt-BR,pt;q=0.9,en;q=0.8,ko;q=0.7,es;q=0.6,fr;q=0.5',
          'baggage': 'sentry-environment=production,sentry-release=c7-lPtjfPTDFX-MNRiUcY,sentry-public_key=c2873b0bbcab403e9bf54ffcab064d1e,sentry-trace_id=e7bfcf47f47245caa429c6a5fa233046,sentry-sample_rate=0,sentry-transaction=%2Fcurrencies%2F%5BcryptocurrencySlug%5D,sentry-sampled=false',
          'referer': 'https://coinmarketcap.com/pt-br/currencies/bitcoin/',
          'sentry-trace': 'e7bfcf47f47245caa429c6a5fa233046-8fbcf3a861bba3f4-0',
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 OPR/127.0.0.0'
        }

        response = request_get(f'https://coinmarketcap.com/pt-br/currencies/{name}/#yields', headers)
        html_page = response.text

        headers = {
          'accept': 'application/json, text/plain, */*',
          'accept-language': 'pt-BR,pt;q=0.9,en;q=0.8,ko;q=0.7,es;q=0.6,fr;q=0.5',
          'dnt': '1',
          'origin': 'https://coinmarketcap.com',
          'platform': 'web',
          'referer': 'https://coinmarketcap.com/',
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 OPR/127.0.0.0',
          'x-request-id': '5a97965872524960862c84e4ad71a22b'
        }

        id = get_substring(html_page, '"id":', ',')
        now = datetime.now()
        date_200_days_ago = now - timedelta(days=200)
        response = request_get(f'https://api.coinmarketcap.com/data-api/v3.1/cryptocurrency/historical?id={id}&convertId=2783&timeStart={int(date_200_days_ago.timestamp())}&timeEnd={int(now.timestamp())}&interval=1d', headers)
        json_historical_data = response.json()

        converted_data = _convert_coinmarketcap_cripto_data(html_page, json_historical_data, info_names)
        log_debug(f'Converted fresh Coin Market Cap data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data from Coin Market Cap for "{name}": {traceback.format_exc()}')
        return None

def _get_data_from_all_sources(name, code, info_names):
    data_binance = _get_data_from_binance(name, code, info_names)
    log_info(f'Data from Binance: {data_binance}')

    missing_binance_infos = filter_remaining_infos(data_binance, info_names)
    log_debug(f'Missing info from Binance: {missing_binance_infos}')

    if data_binance and not missing_binance_infos:
        return data_binance

    data_coinmarketcap = _get_data_from_coinmarketcap(name, code, missing_binance_infos or info_names)
    log_info(f'Data from Coin Market Cap: {data_coinmarketcap}')

    combined_data, missing_combined_infos = combine_data(data_binance, data_coinmarketcap, info_names)
    log_debug(f'Missing info from Binance or Coin Market Cap: {missing_combined_infos}')

    if combined_data and not missing_combined_infos:
        return combined_data

    data_investidor_10 = _get_data_from_investidor10(name, code, missing_combined_infos or info_names)
    log_info(f'Data from Investidor 10: {data_investidor_10}')

    if not data_investidor_10:
        return combined_data

    return { **combined_data, **data_investidor_10 }

def _get_data_from_sources(name, code, source, info_names):
    SOURCES = {
        VALID_CRIPTO_SOURCES['BINANCE_SOURCE']: _get_data_from_binance,
        VALID_CRIPTO_SOURCES['INVESTIDOR10_SOURCE']: _get_data_from_investidor10,
        VALID_CRIPTO_SOURCES['COINMARKETCAP_SOURCE']: _get_data_from_coinmarketcap
    }

    fetch_function = SOURCES.get(source, _get_data_from_all_sources)
    return fetch_function(name, code, info_names)

def get_cripto_data(name, code, source, info_names, can_use_cache):
    cached_data = get_data_from_cache(name, CACHE_FILE_CRIPTO, info_names, can_use_cache)

    SHOULD_UPDATE_CACHE = True

    if not can_use_cache:
        return not SHOULD_UPDATE_CACHE, _get_data_from_sources(name, code, source, info_names)

    missing_cache_info_names = filter_remaining_infos(cached_data, info_names)

    if not missing_cache_info_names:
        return not SHOULD_UPDATE_CACHE, cached_data

    source_data = _get_data_from_sources(name, code, source, missing_cache_info_names)

    if cached_data and source_data:
        return SHOULD_UPDATE_CACHE, { **cached_data, **source_data }
    elif cached_data and not source_data:
        return not SHOULD_UPDATE_CACHE, cached_data
    elif not cached_data and source_data:
        return SHOULD_UPDATE_CACHE, source_data

    return not SHOULD_UPDATE_CACHE, None