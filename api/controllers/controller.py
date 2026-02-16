from flask import Blueprint, jsonify, request

from api.services.br.fii_service import get_fii_data, VALID_FII_INFOS, VALID_FII_SOURCES
from cache.cache_manager import CACHE_FILE_FII, preprocess_cache, upsert_cache
from log.log_manager import log_debug
from utils.utils import get_cache_parameter_info, get_parameter_info

controller_blue_print = Blueprint("controller", __name__, url_prefix="/")

@controller_blue_print.route('/fii/<ticker>', methods=['GET'])
def get_fii_data(ticker):
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
        upsert_cache(ticker, data)

    return jsonify(data), 200


@controller_blue_print.route('/', methods=['GET'])
def get_info():
    return '''
        To get FIIs (BR REITs) infos, access fii/ endpoint passing the ticker name.
    ''', 200