from datetime import datetime
import traceback

from cache.cache_manager import CACHE_FILE_ETF, get_data_from_cache
from log.log_manager import log_debug, log_error, log_info
from utils.utils import (
    filter_remaining_infos,
    get_substring,
    multiply_by_unit,
    remove_type_from_name,
    request_get,
    text_to_number
)

VALID_ETF_SOURCES = {
    'ALL_SOURCE': 'all',
    'INVESTIDOR10_SOURCE': 'investidor10',
    'STOCKANALYSIS_SOURCE': 'stockanalysis'
}

VALID_ETF_INFOS = [
    'actuation',
    'assets_value',
    'avg_annual_dividends',
    'avg_price',
    'beta',
    'cagr_profit',
    'cagr_revenue',
    'debit',
    'dy',
    'ebit',
    'enterprise_value',
    'equity_price',
    'equity_value',
    'gross_margin',
    'initial_date',
    'latests_dividends',
    'link',
    'liquidity',
    'management_fee',
    'market_value',
    'max_52_weeks',
    'min_52_weeks',
    'name',
    'net_margin',
    'net_profit',
    'net_revenue',
    'payout',
    'pl',
    'price',
    'pvp',
    'roe',
    'roic',
    'sector',
    'total_issued_shares',
    'type',
    'variation_12m',
    'variation_30d'
]


def _convert_investidor10_etf_data(html_page, json_dividends_data, info_names):
    patterns_to_remove = [
        '</div>',
        '<div>',
        '<div class="value">',
        '<div class="_card-body">',
        '</span>',
        '<span>',
        '<span class="value">'
    ]

    def get_leatests_dividends(dividends):
        get_leatest_dividend = lambda dividends, year: next((dividend['price'] for dividend in dividends if dividend['created_at'] == year), None)

        current_year = datetime.now().year

        value = get_leatest_dividend(dividends, current_year)

        return value if value else get_leatest_dividend(dividends, current_year -1)

    ALL_INFO = {
        'actuation': lambda: None,
        'assets_value': lambda: multiply_by_unit(get_substring(html_page, 'Capitalização</span>', '</span>', patterns_to_remove)),
        'avg_annual_dividends': lambda: (sum(dividend['price'] for dividend in json_dividends_data) / len(json_dividends_data)) if json_dividends_data else None,
        'avg_price': lambda: None,
        'beta': lambda: None,
        'cagr_profit': lambda: None,
        'cagr_revenue': lambda: None,
        'debit': lambda: None,
        'dy': lambda: text_to_number(get_substring(html_page, 'DY</span>', '</span>', patterns_to_remove), should_convert_thousand_decimal_separators=False),
        'ebit': lambda: None,
        'enterprise_value': lambda: None,
        'equity_price': lambda: None,
        'equity_value': lambda: None,
        'gross_margin': lambda: None,
        'initial_date': lambda: None,
        'latests_dividends': lambda: get_leatests_dividends(json_dividends_data),
        'link': lambda: None,
        'liquidity': lambda: None,
        'management_fee': lambda: None,
        'market_value': lambda: None,
        'max_52_weeks': lambda: None,
        'min_52_weeks': lambda: None,
        'name': lambda: remove_type_from_name(get_substring(html_page, 'name-company">', '<', patterns_to_remove).replace('&amp;', '&')),
        'net_margin': lambda: None,
        'net_profit': lambda: None,
        'net_revenue': lambda: None,
        'payout': lambda: None,
        'pl': lambda: None,
        'price': lambda: text_to_number(get_substring(html_page, '<span class="value">US$', '</span>', patterns_to_remove), should_convert_thousand_decimal_separators=False),
        'pvp': lambda: None,
        'roe': lambda: None,
        'roic': lambda: None,
        'sector': lambda: None,
        'total_issued_shares': lambda: None,
        'total_real_state': lambda: None,
        'type': lambda: 'ETF',
        'vacancy': lambda: None,
        'variation_12m': lambda: text_to_number(get_substring(html_page, 'VARIAÇÃO (12M)</span>', '</span>', patterns_to_remove), should_convert_thousand_decimal_separators=False),
        'variation_30d': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def _get_data_from_investidor10(ticker, info_names):
    try:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'referer': 'https://investidor10.com.br/etfs-global/voo',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        }

        response = request_get(f'https://investidor10.com.br/etfs-global/{ticker}', headers)
        html_page =  response.text[15898:]

        id = get_substring(html_page, 'etfId" value="', '"')

        json_dividends_data = {}
        if 'latests_dividends' in info_names or 'avg_annual_dividends' in info_names:
            response = request_get(f'https://investidor10.com.br/api/etfs/dividendos/chart/{id}/1825/ano', headers)
            json_dividends_data = response.json()

        converted_data = _convert_investidor10_etf_data(html_page, json_dividends_data, info_names)
        log_debug(f'Converted fresh Investidor 10 data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data from Investidor 10 for "{ticker}": {traceback.format_exc()}')
        return None

def _convert_stockanalysis_etf_data(html_page, json_quote_data, info_names):
    def get_leatests_dividends(html_page):
        try:
          paid_dividends = get_substring(html_page, 'dividendTable:[', '],')

          splitted_paid_dividends = paid_dividends.split('},')

          paid_dividends_by_date = { datetime.strptime(get_substring(dividend_data, 'dt:"', '"'), '%Y-%m-%d') : text_to_number(get_substring(dividend_data, 'amt:', ','), should_convert_thousand_decimal_separators=False) for dividend_data in splitted_paid_dividends }

          newest_dividend = max(paid_dividends_by_date)

          return paid_dividends_by_date[newest_dividend]
        except:
          return None

    def get_avg_price(json_quote_data):
        latest_quotes = json_quote_data['data'][-200:]
        return sum([ item[1] for item in latest_quotes ]) / len(latest_quotes)

    equity_value = multiply_by_unit(get_substring(html_page, 'aum:"$', '"'), should_convert_thousand_decimal_separators=False)
    total_issued_shares = multiply_by_unit(get_substring(html_page, 'sharesOut:"', '"'), should_convert_thousand_decimal_separators=False)
    equity_price = equity_value / total_issued_shares
    price = text_to_number(get_substring(html_page, 'cl:', ','), should_convert_thousand_decimal_separators=False)

    ALL_INFO = {
        'actuation': lambda: get_substring(html_page, '"Index Tracked","', '"'),
        'assets_value': lambda: None,
        'avg_annual_dividends': lambda: text_to_number(get_substring(html_page, 'dps:"$', '"'), should_convert_thousand_decimal_separators=False) / 12,
        'avg_price': lambda: get_avg_price(json_quote_data),
        'beta': lambda: text_to_number(get_substring(html_page, 'beta:"', '"'), should_convert_thousand_decimal_separators=False),
        'cagr_profit': lambda: None,
        'cagr_revenue': lambda: None,
        'debit': lambda: None,
        'dy': lambda: text_to_number(get_substring(html_page, 'dividendYield:"', '%"'), should_convert_thousand_decimal_separators=False),
        'ebit': lambda: None,
        'enterprise_value': lambda: None,
        'equity_price': lambda: equity_price,
        'equity_value': lambda: equity_value,
        'gross_margin': lambda: None,
        'initial_date': lambda: get_substring(html_page, 'inception:"', '"'),
        'latests_dividends': lambda: get_leatests_dividends(html_page),
        #'latests_dividends': lambda: text_to_number(get_substring(html_page, 'dps:"$', '",'), should_convert_thousand_decimal_separators=False),
        'link': lambda: get_substring(html_page, 'etf_website:"', '",'),
        'liquidity': lambda: text_to_number(get_substring(html_page, 'v:', ','), should_convert_thousand_decimal_separators=False),
        'management_fee': lambda: text_to_number(get_substring(html_page, 'expenseRatio:"', '%"'), should_convert_thousand_decimal_separators=False),
        'market_value': lambda: None,
        'max_52_weeks': lambda: text_to_number(get_substring(html_page, 'h52:', ','), should_convert_thousand_decimal_separators=False),
        'min_52_weeks': lambda: text_to_number(get_substring(html_page, 'l52:', ','), should_convert_thousand_decimal_separators=False),
        'name': lambda: remove_type_from_name(get_substring(get_substring(html_page, 'info:{', '},'), 'name:"', '"')),
        'net_margin': lambda: None,
        'net_profit': lambda: None,
        'net_revenue': lambda: None,
        'payout': lambda: text_to_number(get_substring(html_page, 'payoutRatio:"', '%"'), should_convert_thousand_decimal_separators=False),
        'pl': lambda: text_to_number(get_substring(html_page, 'peRatio:"', '"'), should_convert_thousand_decimal_separators=False),
        'price': lambda: price,
        'pvp': lambda: price / equity_price,
        'roe': lambda: None,
        'roic': lambda: None,
        'sector': lambda: str(get_substring(html_page, '"Asset Class","', '"')) + '/' + str(get_substring(html_page, '"Category","', '"')),
        'total_issued_shares': lambda: total_issued_shares,
        'total_real_state': lambda: None,
        'type': lambda: 'ETF',
        'vacancy': lambda: None,
        'variation_12m': lambda: text_to_number(get_substring(html_page, 'ch1y:"', '"'), should_convert_thousand_decimal_separators=False),
        'variation_30d': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def _get_data_from_stockanalysis(ticker, info_names):
    try:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'referer': 'https://stockanalysis.com/',
            'upgrade-insecure-requests': '1',
            'priority': 'u=0, i',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 OPR/118.0.0.0',
        }

        response = request_get(f'https://stockanalysis.com/etf/{ticker}', headers)
        html_page = get_substring(response.text[4_000:], 'Promise.all([', 'news:')

        response = request_get(f'https://stockanalysis.com/api/symbol/e/{ticker}/history?type=chart', headers)
        json_quote_data = response.json()

        converted_data = _convert_stockanalysis_etf_data(html_page, json_quote_data, info_names)
        log_debug(f'Converted fresh Stock Analysis data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data from Stock Analysis for "{ticker}": {traceback.format_exc()}')
        return None

def _get_data_from_all_sources(ticker, info_names):
    data_stockanalysis = _get_data_from_stockanalysis(ticker, info_names)
    log_info(f'Data from Stock Analysis: {data_stockanalysis}')

    missing_stockanalysis_infos = filter_remaining_infos(data_stockanalysis, info_names)
    log_debug(f'Missing info from Stock Analysis: {missing_stockanalysis_infos}')

    if data_stockanalysis and not missing_stockanalysis_infos:
        return data_stockanalysis

    data_investidor_10 = _get_data_from_investidor10(ticker, missing_stockanalysis_infos or info_names)
    log_info(f'Data from Investidor 10: {data_investidor_10}')

    if not data_investidor_10:
        return data_stockanalysis

    return { **data_stockanalysis, **data_investidor_10 }

def _get_data_from_sources(ticker, source, info_names):
    SOURCES = {
        VALID_ETF_SOURCES['STOCKANALYSIS_SOURCE']: _get_data_from_stockanalysis,
        VALID_ETF_SOURCES['INVESTIDOR10_SOURCE']: _get_data_from_investidor10
    }

    fetch_function = SOURCES.get(source, _get_data_from_all_sources)
    return fetch_function(ticker, info_names)

def get_etf_data(ticker, source, info_names, can_use_cache):
    cached_data = get_data_from_cache(ticker, CACHE_FILE_ETF, info_names, can_use_cache)

    SHOULD_UPDATE_CACHE = True

    if not can_use_cache:
        return not SHOULD_UPDATE_CACHE, _get_data_from_sources(ticker, source, info_names)

    missing_cache_info_names = filter_remaining_infos(cached_data, info_names)

    if not missing_cache_info_names:
        return not SHOULD_UPDATE_CACHE, cached_data

    source_data = _get_data_from_sources(ticker, source, missing_cache_info_names)

    if cached_data and source_data:
        return SHOULD_UPDATE_CACHE, { **cached_data, **source_data }
    elif cached_data and not source_data:
        return not SHOULD_UPDATE_CACHE, cached_data
    elif not cached_data and source_data:
        return SHOULD_UPDATE_CACHE, source_data

    return not SHOULD_UPDATE_CACHE, None