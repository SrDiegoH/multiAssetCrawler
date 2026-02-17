from flask import Blueprint, jsonify, request

from api.services.internacional.cripto_service import get_cripto_data, VALID_CRIPTO_INFOS, VALID_CRIPTO_SOURCES
from api.services.internacional.etf_service import get_etf_data, VALID_ETF_INFOS, VALID_ETF_SOURCES
from api.services.internacional.stock_service import get_stock_data, VALID_STOCK_INFOS, VALID_STOCK_SOURCES
from api.services.internacional.reit_service import get_reit_data, VALID_REIT_INFOS, VALID_REIT_SOURCES
from api.services.nacional.acao_service import get_acao_data, VALID_ACAO_INFOS, VALID_ACAO_SOURCES
from api.services.nacional.fii_service import get_fii_data, VALID_FII_INFOS, VALID_FII_SOURCES
from cache.cache_manager import (
    CACHE_FILE_ACAO,
    CACHE_FILE_CRIPTO,
    CACHE_FILE_ETF,
    CACHE_FILE_FII,
    CACHE_FILE_STOCK,
    CACHE_FILE_REIT,
    preprocess_cache,
    upsert_cache,
)
from log.log_manager import log_debug
from utils.utils import get_cache_parameter_info, get_parameter_info, VALID_ASSET_CLASSES_MAPPER

controller_blue_print = Blueprint("controller", __name__, url_prefix="/")

_VALID_ASSET_CLASSES_MAPPER = {
    'ação'  : (VALID_ACAO_INFOS, VALID_ACAO_SOURCES),
    'acao'  : (VALID_ACAO_INFOS, VALID_ACAO_SOURCES),
    'ações' : (VALID_ACAO_INFOS, VALID_ACAO_SOURCES),
    'acoes' : (VALID_ACAO_INFOS, VALID_ACAO_SOURCES),
    'cripto': (VALID_CRIPTO_INFOS, VALID_CRIPTO_SOURCES),
    'cryptocurrency'  : (VALID_CRIPTO_INFOS, VALID_CRIPTO_SOURCES),
    'cryptocurrencies': (VALID_CRIPTO_INFOS, VALID_CRIPTO_SOURCES),
    'etf'  : (VALID_ETF_INFOS, VALID_ETF_SOURCES),
    'fii'  : (VALID_FII_INFOS, VALID_FII_SOURCES),
    'stock': (VALID_STOCK_INFOS, VALID_STOCK_SOURCES),
    'reit' : (VALID_REIT_INFOS, VALID_REIT_SOURCES),
}

@controller_blue_print.route('/acao/<ticker>', methods=['GET'])
def crawl_acao_data(ticker):
    should_delete_all_cache = get_cache_parameter_info(request.args, 'should_delete_all_cache')
    should_clear_cached_data = get_cache_parameter_info(request.args, 'should_clear_cached_data')
    should_use_cache = get_cache_parameter_info(request.args, 'should_use_cache', '1')

    ticker = ticker.upper()

    raw_source = get_parameter_info(request.args, 'source', VALID_ACAO_SOURCES['ALL_SOURCE'])
    source = raw_source if raw_source in VALID_ACAO_SOURCES.values() else VALID_ACAO_SOURCES['ALL_SOURCE']

    raw_info_names = [ info for info in get_parameter_info(request.args, 'info_names', '').split(',') if info in VALID_ACAO_INFOS ]
    info_names = raw_info_names if len(raw_info_names) else VALID_ACAO_INFOS

    log_debug(f'Should Delete cache? {should_delete_all_cache} - Should Clear cache? {should_clear_cached_data} - Should Use cache? {should_use_cache}')
    log_debug(f'Ticker: {ticker} - Source: {source} - Info names: {info_names}')

    can_use_cache = preprocess_cache(ticker, CACHE_FILE_ACAO, should_delete_all_cache, should_clear_cached_data, should_use_cache)

    should_update_cache, data = get_acao_data(ticker, source, info_names, can_use_cache)

    log_debug(f'Final Data: {data}')

    if not data:
        return jsonify({ 'error': 'No data found' }), 404

    if can_use_cache and should_update_cache:
        upsert_cache(ticker, CACHE_FILE_ACAO, data)

    return jsonify(data), 200

@controller_blue_print.route('/cripto/<name>/<code>', methods=['GET'])
def crawl_cripto_data(name, code):
    should_delete_all_cache = get_cache_parameter_info(request.args, 'should_delete_all_cache')
    should_clear_cached_data = get_cache_parameter_info(request.args, 'should_clear_cached_data')
    should_use_cache = get_cache_parameter_info(request.args, 'should_use_cache', '1')

    name = name.lower()
    code = code.upper()

    raw_source = get_parameter_info(request.args, 'source', VALID_CRIPTO_SOURCES['ALL_SOURCE'])
    source = raw_source if raw_source in VALID_CRIPTO_SOURCES.values() else VALID_CRIPTO_SOURCES['ALL_SOURCE']

    raw_info_names = [ info for info in get_parameter_info(request.args, 'info_names', '').split(',') if info in VALID_CRIPTO_INFOS ]
    info_names = raw_info_names if len(raw_info_names) else VALID_CRIPTO_INFOS

    log_debug(f'Should Delete cache? {should_delete_all_cache} - Should Clear cache? {should_clear_cached_data} - Should Use cache? {should_use_cache}')
    log_debug(f'Cripto: {name} - {code} - Source: {source} - Info names: {info_names}')

    can_use_cache = preprocess_cache(name, CACHE_FILE_CRIPTO, should_delete_all_cache, should_clear_cached_data, should_use_cache)

    should_update_cache, data = get_cripto_data(name, code, source, info_names, can_use_cache)

    log_debug(f'Final Data: {data}')

    if not data:
        return jsonify({ 'error': 'No data found' }), 404

    if can_use_cache and should_update_cache:
        upsert_cache(name, CACHE_FILE_CRIPTO, data)

    return jsonify(data), 200

@controller_blue_print.route('/etf/<ticker>', methods=['GET'])
def crawl_etf_data(ticker):
    should_delete_all_cache = get_cache_parameter_info(request.args, 'should_delete_all_cache')
    should_clear_cached_data = get_cache_parameter_info(request.args, 'should_clear_cached_data')
    should_use_cache = get_cache_parameter_info(request.args, 'should_use_cache', '1')

    ticker = ticker.upper()

    raw_source = get_parameter_info(request.args, 'source', VALID_ETF_SOURCES['ALL_SOURCE'])
    source = raw_source if raw_source in VALID_ETF_SOURCES.values() else VALID_ETF_SOURCES['ALL_SOURCE']

    raw_info_names = [ info for info in get_parameter_info(request.args, 'info_names', '').split(',') if info in VALID_ETF_INFOS ]
    info_names = raw_info_names if len(raw_info_names) else VALID_ETF_INFOS

    log_debug(f'Should Delete cache? {should_delete_all_cache} - Should Clear cache? {should_clear_cached_data} - Should Use cache? {should_use_cache}')
    log_debug(f'Ticker: {ticker} - Source: {source} - Info names: {info_names}')

    can_use_cache = preprocess_cache(ticker, CACHE_FILE_ETF, should_delete_all_cache, should_clear_cached_data, should_use_cache)

    should_update_cache, data = get_etf_data(ticker, source, info_names, can_use_cache)

    log_debug(f'Final Data: {data}')

    if not data:
        return jsonify({ 'error': 'No data found' }), 404

    if can_use_cache and should_update_cache:
        upsert_cache(ticker, CACHE_FILE_ETF, data)

    return jsonify(data), 200

@controller_blue_print.route('/fii/<ticker>', methods=['GET'])
def crawl_fii_data(ticker):
    should_delete_all_cache = get_cache_parameter_info(request.args, 'should_delete_all_cache')
    should_clear_cached_data = get_cache_parameter_info(request.args, 'should_clear_cached_data')
    should_use_cache = get_cache_parameter_info(request.args, 'should_use_cache', '1')

    ticker = ticker.upper()

    raw_source = get_parameter_info(request.args, 'source', VALID_FII_SOURCES['ALL_SOURCE'])
    source = raw_source if raw_source in VALID_FII_SOURCES.values() else VALID_FII_SOURCES['ALL_SOURCE']

    raw_info_names = [ info for info in get_parameter_info(request.args, 'info_names', '').split(',') if info in VALID_FII_INFOS ]
    info_names = raw_info_names if len(raw_info_names) else VALID_FII_INFOS

    log_debug(f'Should Delete cache? {should_delete_all_cache} - Should Clear cache? {should_clear_cached_data} - Should Use cache? {should_use_cache}')
    log_debug(f'Ticker: {ticker} - Source: {source} - Info names: {info_names}')

    can_use_cache = preprocess_cache(ticker, CACHE_FILE_FII, should_delete_all_cache, should_clear_cached_data, should_use_cache)

    should_update_cache, data = get_fii_data(ticker, source, info_names, can_use_cache)

    log_debug(f'Final Data: {data}')

    if not data:
        return jsonify({ 'error': 'No data found' }), 404

    if can_use_cache and should_update_cache:
        upsert_cache(ticker, CACHE_FILE_FII, data)

    return jsonify(data), 200

@controller_blue_print.route('/stock/<ticker>', methods=['GET'])
def crawl_stock_data(ticker):
    should_delete_all_cache = get_cache_parameter_info(request.args, 'should_delete_all_cache')
    should_clear_cached_data = get_cache_parameter_info(request.args, 'should_clear_cached_data')
    should_use_cache = get_cache_parameter_info(request.args, 'should_use_cache', '1')

    ticker = ticker.upper()

    raw_source = get_parameter_info(request.args, 'source', VALID_STOCK_SOURCES['ALL_SOURCE'])
    source = raw_source if raw_source in VALID_STOCK_SOURCES.values() else VALID_STOCK_SOURCES['ALL_SOURCE']

    raw_info_names = [ info for info in get_parameter_info(request.args, 'info_names', '').split(',') if info in VALID_STOCK_INFOS ]
    info_names = raw_info_names if len(raw_info_names) else VALID_STOCK_INFOS

    log_debug(f'Should Delete cache? {should_delete_all_cache} - Should Clear cache? {should_clear_cached_data} - Should Use cache? {should_use_cache}')
    log_debug(f'Ticker: {ticker} - Source: {source} - Info names: {info_names}')

    can_use_cache = preprocess_cache(ticker, CACHE_FILE_STOCK, should_delete_all_cache, should_clear_cached_data, should_use_cache)

    should_update_cache, data = get_stock_data(ticker, source, info_names, can_use_cache)

    log_debug(f'Final Data: {data}')

    if not data:
        return jsonify({ 'error': 'No data found' }), 404

    if can_use_cache and should_update_cache:
        upsert_cache(ticker, CACHE_FILE_STOCK, data)

    return jsonify(data), 200

@controller_blue_print.route('/reit/<ticker>', methods=['GET'])
def crawl_reit_data(ticker):
    should_delete_all_cache = get_cache_parameter_info(request.args, 'should_delete_all_cache')
    should_clear_cached_data = get_cache_parameter_info(request.args, 'should_clear_cached_data')
    should_use_cache = get_cache_parameter_info(request.args, 'should_use_cache', '1')

    ticker = ticker.upper()

    raw_source = get_parameter_info(request.args, 'source', VALID_REIT_SOURCES['ALL_SOURCE'])
    source = raw_source if raw_source in VALID_REIT_SOURCES.values() else VALID_REIT_SOURCES['ALL_SOURCE']

    raw_info_names = [ info for info in get_parameter_info(request.args, 'info_names', '').split(',') if info in VALID_REIT_INFOS ]
    info_names = raw_info_names if len(raw_info_names) else VALID_REIT_INFOS

    log_debug(f'Should Delete cache? {should_delete_all_cache} - Should Clear cache? {should_clear_cached_data} - Should Use cache? {should_use_cache}')
    log_debug(f'Ticker: {ticker} - Source: {source} - Info names: {info_names}')

    can_use_cache = preprocess_cache(ticker, CACHE_FILE_REIT, should_delete_all_cache, should_clear_cached_data, should_use_cache)

    should_update_cache, data = get_reit_data(ticker, source, info_names, can_use_cache)

    log_debug(f'Final Data: {data}')

    if not data:
        return jsonify({ 'error': 'No data found' }), 404

    if can_use_cache and should_update_cache:
        upsert_cache(ticker, CACHE_FILE_REIT, data)

    return jsonify(data), 200

@controller_blue_print.route('/', methods=['GET'])
def info():
    return '''
        To get <strong>Ações</strong> (Brazilian stocks) informations, access the <code>acao/</code> endpoint and pass the <strong>ticker</strong> in the path.</br>
        To get <strong>Cryptocurrencies</strong> informations, access the <code>cripto/</code> endpoint and pass the <strong>name</strong> and <strong>code</strong> respectively in the path.</br>
        To get <strong>ETFs</strong> informations, access the <code>etf/</code> endpoint and pass the <strong>ticker</strong> in the path.</br>
        To get <strong>FIIs</strong> (Brazilian REIT-like funds) informations, access the <code>fii/</code> endpoint and pass the <strong>ticker</strong> in the path.</br>
        To get <strong>Stocks</strong> informations, access the <code>stock/</code> endpoint and pass the <strong>ticker</strong> in the path.</br>
        To get <strong>REITs</strong> informations, access the <code>reit/</code> endpoint and pass the <strong>ticker</strong> in the path.</br>
        </br></br>
        By default, cached data is used. To fetch fresh data, pass the query parameter <code>should_use_cache</code> as <strong>0</strong>.
        To clear cached data for a specific asset, pass the query parameter <code>should_clear_cached_data</code> as <strong>1</strong>.
        To delete all cached data for a specific asset class, pass the query parameter <code>should_delete_all_cache</code> as <strong>1</strong>.
        <em>PS: It is highly recommended to use caching to prevent abuse of sources.</em>
        </br></br>
        By default, all valid information is returned. To filter specific informaton, pass the <strong>informaton names</strong> as a query parameter separeted by <code>,</code> (comma).</b>
        To see all valid information names for a specific asset class, access <code>valid-infos/</code> and pass the <strong>asset name</strong> in the path.
        </br></br>
        By default, data from all sources is returned. To crawl from a specific source, pass the <strong>source names</strong> as a query parameter.</b>
        To see all valid sources for a specific asset class, access <code>valid-sources/</code> and pass the <strong>asset name</strong> in the path.
    ''', 200

def _resolve_asset_class(asset_class, is_info_request=True):
    normalized = asset_class.strip().lower()
    index = 0 if is_info_request else 1

    for key, value in _VALID_ASSET_CLASSES_MAPPER.items():
        if key in normalized:
            return jsonify(value[index]), 200

    return jsonify({ 'error': 'Not valid Asset Class' }), 400

@controller_blue_print.route('/valid-infos/<asset_class>', methods=['GET'])
def valid_info(asset_class):
    return _resolve_asset_class(asset_class)

@controller_blue_print.route('/valid-sources/<asset_class>', methods=['GET'])
def valid_info(asset_class):
    return _resolve_asset_class(asset_class, is_info_request=False)