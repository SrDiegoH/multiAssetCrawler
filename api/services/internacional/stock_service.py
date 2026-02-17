from datetime import datetime
import json
import traceback

from datetime import datetime
import traceback

from cache.cache_manager import CACHE_FILE_STOCK, get_data_from_cache
from log.log_manager import log_debug, log_error, log_info
from utils.utils import (
    filter_remaining_infos,
    get_substring,
    multiply_by_unit,
    remove_type_from_name,
    request_get,
    text_to_number
)

VALID_STOCK_SOURCES = {
    'ALL_SOURCE': 'all',
    'INVESTIDOR10_SOURCE': 'investidor10',
    'STOCKANALYSIS_SOURCE': 'stockanalysis'
}

VALID_STOCK_INFOS = [
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

def _convert_investidor10_stock_data(json_ticker_page, json_dividends_data, info_names):
    balance = max(json_ticker_page['balances'], key=lambda balance: datetime.strptime(balance['reference_date'], "%Y-%m-%dT%H:%M:%S.%fZ"))
    actual_price = max(json_ticker_page['quotations'], key=lambda quotation: datetime.strptime(quotation['date'], "%Y-%m-%dT%H:%M:%S.%fZ"))['price']

    def get_leatests_dividends(dividends):
        get_leatest_dividend = lambda dividends, year: next((dividend['price'] for dividend in dividends if dividend['created_at'] == year), None)

        current_year = datetime.now().year

        value = get_leatest_dividend(dividends, current_year)

        return value if value else get_leatest_dividend(dividends, current_year -1)

    ALL_INFO = {
        'actuation': lambda: json_ticker_page['industry']['name'],
        'assets_value': lambda: balance['total_assets'],
        'avg_annual_dividends': lambda: (sum(dividend['price'] for dividend in json_dividends_data) / len(json_dividends_data)) if json_dividends_data else None,
        'avg_price': lambda: None,
        'beta': lambda: None,
        'cagr_profit': lambda: balance['growth_net_profit_last_5_years'],
        'cagr_revenue': lambda: balance['growth_net_revenue_last_5_years'],
        'debit': lambda: text_to_number(balance['long_term_debt']),
        'dy': lambda: text_to_number(balance['dy']),
        'ebit': lambda: text_to_number(balance['ebit']),
        'enterprise_value': lambda: None,
        'equity_price': lambda: None,
        'equity_value': lambda: balance['total_equity'],
        'gross_margin': lambda: text_to_number(balance['gross_margin']),
        'initial_date': lambda: json_ticker_page['start_year_on_stock_exchange'],
        'latests_dividends': lambda: get_leatests_dividends(json_dividends_data),
        'link': lambda: None,
        'liquidity': lambda: balance['volume_avg'],
        'management_fee': lambda: None,
        'market_value': lambda: balance['market_cap'],
        'max_52_weeks': lambda: None,
        'min_52_weeks': lambda: None,
        'name': lambda: remove_type_from_name(json_ticker_page['company_name']),
        'net_margin': lambda: text_to_number(balance['net_margin']),
        'net_profit': lambda: balance['net_income'],
        'net_revenue': lambda: balance['revenue'],
        'payout': lambda: text_to_number(balance['api_info']['common_size_ratios']['dividend_payout_ratio']),
        'pl': lambda: text_to_number(balance['pl']),
        'price': lambda: actual_price,
        'pvp': lambda: text_to_number(balance['pvp']),
        'roe': lambda: text_to_number(balance['roe']),
        'roic': lambda: text_to_number(balance['roic']),
        'sector': lambda: json_ticker_page['industry']['sector']['name'],
        'total_issued_shares': lambda: balance['shares_outstanding'],
        'total_real_state': lambda: None,
        'type': lambda: json_ticker_page['type'],
        'vacancy': lambda: None,
        'variation_12m': lambda: balance['variation_year'],
        'variation_30d': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def _get_stock_from_investidor10(ticker, info_names):
    try:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'referer': 'https://investidor10.com.br/reits/0/',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        }

        response = request_get(f'https://investidor10.com.br/stock/{ticker}', headers)
        html_page =  response.text[15898:]

        json_data = get_substring(html_page, 'var mainTicker =', 'var ')[:-1]
        json_ticker_page  = json.loads(json_data)

        json_dividends_data = {}
        if 'latests_dividends' in info_names or 'avg_annual_dividends' in info_names:
            response = request_get(f'https://investidor10.com.br/api/stock/dividendos/chart/{json_ticker_page["id"]}/3650/ano', headers)
            json_dividends_data = response.json()

        converted_data = _convert_investidor10_stock_data(json_ticker_page, json_dividends_data, info_names)
        log_debug(f'Converted fresh Investidor 10 data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data from Investidor 10 for "{ticker}": {traceback.format_exc()}')
        return None

def _convert_stockanalysis_stock_data(ticker, initial_page, statistics_page, info_names):
    roa = text_to_number(get_substring(statistics_page, 'ROA)",value:"', '%'))
    net_profit = multiply_by_unit(get_substring(initial_page, 'netIncome:"', '"'))

    avg_annual_dividends = text_to_number(get_substring(statistics_page, 'Dividend Per Share",value:"$', '"'))

    ALL_INFO = {
        'actuation': lambda: get_substring(initial_page, 'Industry",v:"', '"'),
        'assets_value': lambda: net_profit / roa,
        'avg_annual_dividends': lambda: avg_annual_dividends,
        'avg_price': lambda: text_to_number(get_substring(statistics_page, '200-Day Moving Average",value:"', '"')),
        'beta': lambda: get_substring(statistics_page, 'Beta (5Y)",value:"', '"'),
        'cagr_profit': lambda: None,
        'cagr_revenue': lambda: None,
        'debit': lambda: multiply_by_unit(get_substring(statistics_page, 'Debt",value:"', '"')),
        'dy': lambda: text_to_number(get_substring(statistics_page, 'Dividend Yield",value:"', '%')),
        'ebit': lambda: multiply_by_unit(get_substring(statistics_page, 'EBIT",value:"', '"')),
        'enterprise_value': lambda: multiply_by_unit(get_substring(statistics_page, 'Enterprise Value",value:"', '",')),
        'equity_price': lambda: None,
        'equity_value': lambda: None,
        'gross_margin': lambda: multiply_by_unit(get_substring(statistics_page, 'Gross Margin",value:"', '%')),
        'initial_date': lambda: get_substring(initial_page, 'inception:"', '"'),
        'latests_dividends': lambda: avg_annual_dividends / 12,
        'link': lambda: f'https://stockanalysis.com/stocks/{ticker}/company/',
        #'link': lambda: get_substring(initial_page, 'Website",v:"', '",'),
        'liquidity': lambda: text_to_number(get_substring(statistics_page, 'Average Volume (20 Days)",value:"', '"')),
        #'liquidity': lambda: get_substring(initial_page, 'v:', '",'),
        'management_fee': lambda: None,
        'market_value': lambda: multiply_by_unit(get_substring(statistics_page, 'Market Cap",value:"', '"')),
        'max_52_weeks': lambda: get_substring(initial_page, 'h52:', ','),
        'min_52_weeks': lambda: get_substring(initial_page, 'l52:', ','),
        'name': lambda: get_substring(initial_page, 'nameFull:"', '"'),
        'net_margin': lambda: multiply_by_unit(get_substring(statistics_page, 'Operating Margin",value:"', '%')),
        'net_profit': lambda: net_profit,
        'net_revenue': lambda: multiply_by_unit(get_substring(initial_page, 'revenue:"', '"')),
        'payout': lambda: text_to_number(get_substring(statistics_page, 'Payout Ratio",value:"', '%')),
        'pl': lambda: get_substring(initial_page, 'peRatio:"', '"'),
        'price': lambda: get_substring(initial_page, 'cl:', ','),
        'pvp': lambda: None,
        'roe': lambda: text_to_number(get_substring(statistics_page, 'ROE)",value:"', '%')),
        'roic': lambda: text_to_number(get_substring(statistics_page, 'ROIC)",value:"', '%')),
        'sector': lambda: get_substring(initial_page, 'Sector",v:"', '",'),
        'total_issued_shares': lambda: multiply_by_unit(get_substring(initial_page, 'sharesOut:"', '"')),
        'total_real_state': lambda: None,
        'type': lambda: 'STOCK',
        'vacancy': lambda: None,
        'variation_12m': lambda: text_to_number(get_substring(statistics_page, '52-Week Price Change",value:"', '%')),
        'variation_30d': lambda: None,
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def _get_stock_from_stockanalysis(ticker, info_names):
    try:
        headers = {
            'accept': '*/*',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'no-cache',
            'dnt': '1',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'referer': 'https://stockanalysis.com/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 OPR/118.0.0.0',
        }

        response =  request_get(f'https://stockanalysis.com/stocks/{ticker}', headers)
        initial_page = get_substring(response.text[5_000:], 'Promise.all([', 'news:')

        response =  request_get(f'https://stockanalysis.com/stocks/{ticker}/statistics', headers)
        statistics_page = get_substring(response.text[5_000:], 'Promise.all([', ';')

        converted_data = _convert_stockanalysis_stock_data(ticker, initial_page, statistics_page, info_names)
        log_debug(f'Converted fresh Stock Analysis data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data from Stock Analysis for "{ticker}": {traceback.format_exc()}')
        return None

def _get_stock_from_all_sources(ticker, info_names):
    data_stockanalysis = _get_stock_from_stockanalysis(ticker, info_names)
    log_info(f'Data from Stock Analysis: {data_stockanalysis}')

    missing_stockanalysis_infos = filter_remaining_infos(data_stockanalysis, info_names)
    log_debug(f'Missing info from Stock Analysis: {missing_stockanalysis_infos}')

    if data_stockanalysis and not missing_stockanalysis_infos:
        return data_stockanalysis

    data_investidor_10 = _get_stock_from_investidor10(ticker, missing_stockanalysis_infos or info_names)
    log_info(f'Data from Investidor 10: {data_investidor_10}')

    if not data_investidor_10:
        return data_stockanalysis

    return { **data_stockanalysis, **data_investidor_10 }

def _get_data_from_sources(ticker, source, info_names):
    SOURCES = {
        VALID_STOCK_SOURCES['STOCKANALYSIS_SOURCE']: _get_stock_from_stockanalysis,
        VALID_STOCK_SOURCES['INVESTIDOR10_SOURCE']: _get_stock_from_investidor10
    }

    fetch_function = SOURCES.get(source, _get_stock_from_all_sources)
    return fetch_function(ticker, info_names)

def get_stock_data(ticker, source, info_names, can_use_cache):
    cached_data = get_data_from_cache(ticker, CACHE_FILE_STOCK, info_names, can_use_cache)

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