import base64
from datetime import datetime
from html import unescape
import json
import traceback

from cache.cache_manager import CACHE_FILE_FII, get_data_from_cache
from log.log_manager import log_debug, log_error, log_info
from utils.utils import (
    combine_data,
    filter_remaining_infos,
    get_substring,
    multiply_by_unit,
    request_get,
    text_to_number,
)

VALID_FII_SOURCES = {
    'ALL_SOURCE': 'all',
    'BMFBOVESPA_SOURCE': 'bmfbovespa',
    'FIIS_SOURCE': 'fiis',
    'FUNDAMENTUS_SOURCE': 'fundamentus',
    'FUNDSEXPLORER_SOURCE': 'fundsexplorer',
    'INVESTIDOR10_SOURCE': 'investidor10'
}

VALID_FII_INFOS = [
    'actuation',
    'assets_value',
    'avg_price',
    'cash_value',
    'debit_by_real_state_acquisition',
    'debit_by_securitization_receivables_acquisition',
    'dy',
    'equity_price',
    'ffoy',
    'initial_date',
    'latest_dividend',
    'latests_dividends',
    'link',
    'liquidity',
    'management',
    'market_value',
    'max_52_weeks',
    'mayer_multiple',
    'min_52_weeks',
    'name',
    'net_equity_value',
    'price',
    'pvp',
    'segment',
    'target_public',
    'term',
    'total_issued_shares',
    'total_mortgage_value',
    'total_mortgage',
    'total_real_state_value',
    'total_real_state',
    'total_stocks_fund_others_value',
    'total_stocks_fund_others',
    'type',
    'vacancy',
    'variation_12m',
    'variation_30d'
]

_investidor_10_preloaded_data = (None, None)
_fundamentus_preloaded_data = (None, None)
_fiis_preloaded_data = (None, None)

def _convert_bmfbovespa_data(IME_doc, ITE_doc, RA_docs, cnpj, info_names):
    patterns_to_remove = [
        '</b>',
        '</span>',
        '</td>',
        '<b>',
        '<center>',
        '<span class="dado-cabecalho">',
        '<span class="dado-valores">',
        '<span>',
        '<td align="center">',
        '<td>'
    ]

    def count_total_real_state_value():
        return text_to_number(get_substring(IME_doc[0], 'Direitos reais sobre bens im&oacute;veis ', '</span>', patterns_to_remove))

    def count_total_mortgage_value():
        return (
            text_to_number(get_substring(IME_doc[0], 'Certificados de Dep&oacute;sitos de Valores Mobili&aacute;rios', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(IME_doc[0], 'Notas Promiss&oacute;rias', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(IME_doc[0], 'Notas Comerciais', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(IME_doc[0], 'CRI" (se FIAGRO, Certificado de Receb&iacute;veis do Agroneg&oacute;cio "CRA")', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(IME_doc[0], 'Hipotec&aacute;rias', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(IME_doc[0], 'LCI" (se FIAGRO, Letras de Cr&eacute;dito do Agroneg&oacute;cio "LCA")', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(IME_doc[0], 'LIG)', '</span>', patterns_to_remove))
        )

    def count_total_stocks_fund_others_value():
        return (
                text_to_number(get_substring(IME_doc[0], 'A&ccedil;&otilde;es', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'Deb&ecirc;ntures', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'certificados de desdobramentos', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'FIA)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'FIP)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'FII)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'FIDC)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'Outras cotas de Fundos de Investimento', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'A&ccedil;&otilde;es de Sociedades cujo &uacute;nico prop&oacute;sito se enquadra entre as atividades permitidas aos FII', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'Cotas de Sociedades que se enquadre entre as atividades permitidas aos FII', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'CEPAC)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'Outros Valores Mobili&aacute;rios', '</span>', patterns_to_remove))
            )

    def fii_type():
        fii_type = {
            'Outro': count_total_stocks_fund_others_value(),
            'Papel': count_total_mortgage_value(),
            'Tijolo': count_total_real_state_value()
        }

        return max(fii_type, key=fii_type.get)

    ALL_INFO = {
        'actuation': lambda: None,
        'assets_value': lambda: text_to_number(get_substring(IME_doc[0], 'Ativo &ndash; R$', '</span>', patterns_to_remove)),
        'avg_price': lambda: None,
        'cash_value': lambda: text_to_number(get_substring(IME_doc[0], 'Total mantido para as Necessidades de Liquidez (art. 46, &sect; &uacute;nico, ICVM 472/08) </b>', '</span>', patterns_to_remove)),
        'debit_by_real_state_acquisition': lambda: text_to_number(get_substring(IME_doc[0], 'Obriga&ccedil;&otilde;es por aquisi&ccedil;&atilde;o de im&oacute;veis', '</span>', patterns_to_remove)),
        'debit_by_securitization_receivables_acquisition': lambda: text_to_number(get_substring(IME_doc[0], 'Obriga&ccedil;&otilde;es por securitiza&ccedil;&atilde;o de receb&iacute;veis', '</span>', patterns_to_remove)),
        'dy': lambda: None,
        'equity_price': lambda: text_to_number(get_substring(IME_doc[0], 'Valor Patrimonial das Cotas &ndash; R$', '</span>', patterns_to_remove)),
        'ffoy': lambda: None,
        'initial_date': lambda: get_substring(IME_doc[0], 'doc de Funcionamento:', '</span>', patterns_to_remove),
        'latest_dividend': lambda: RA_docs[max(RA_docs.keys(), key=lambda date: datetime.strptime(date, "%d%m%Y"))] if len(RA_docs) else None,
        'latests_dividends': lambda: sum(RA_docs.values()),
        'link': lambda: f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={cnpj}#',
        'liquidity': lambda: None,
        'management': lambda: get_substring(IME_doc[0], 'Tipo de Gest&atilde;o:', '</span>', patterns_to_remove),
        'market_value': lambda: None,
        'max_52_weeks': lambda: None,
        'mayer_multiple': lambda: None,
        'min_52_weeks': lambda: None,
        'name': lambda: unescape(get_substring(IME_doc[0], 'Nome do Fundo/Classe: </span>', '</span>', patterns_to_remove)),
        'net_equity_value': lambda: text_to_number(get_substring(IME_doc[0], 'Patrim&ocirc;nio L&iacute;quido &ndash; R$', '</span>', patterns_to_remove)),
        'price': lambda: None,
        'pvp': lambda: None,
        'segment': lambda: unescape(get_substring(IME_doc[0], 'Segmento de Atua&ccedil;&atilde;o:', '</span>', patterns_to_remove)),
        'target_public': lambda: get_substring(IME_doc[0], 'P&uacute;blico Alvo: </span>', '</span>', patterns_to_remove),
        'term': lambda: get_substring(IME_doc[0], '>Prazo de Dura&ccedil;&atilde;o: </span>', '</span>', patterns_to_remove),
        'total_issued_shares': lambda: text_to_number(get_substring(IME_doc[0], 'Quantidade de cotas emitidas: </span>', '</span>', patterns_to_remove)),
        'total_mortgage': lambda: get_substring(ITE_doc[0], ' 1.2.2', '1.2.6', patterns_to_remove).count('</tr>') - 8,
        'total_mortgage_value': count_total_mortgage_value,
        'total_real_state': lambda: ITE_doc[0].count('&Aacute;rea (m2):') + (get_substring(ITE_doc[0], '1.1.1', '>1.1.2<', patterns_to_remove).count('</tr>') - 2),
        'total_real_state_value': count_total_real_state_value,
        'total_stocks_fund_others': lambda: (get_substring(ITE_doc[0], ' 1.2.1', ' 1.2.2', patterns_to_remove).count('</tr>') - 2) + (get_substring(ITE_doc[0], ' 1.2.6', '>1.3<', patterns_to_remove).count('</tr>') - 16),
        'total_stocks_fund_others_value': count_total_stocks_fund_others_value,
        'type': fii_type,
        'vacancy': lambda: None,
        'variation_12m': lambda: None,
        'variation_30d': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def _fetch_documents(cnpj, document_configs):
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01, text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
        'Referer': 'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0',
        'X-Requested-With': 'XMLHttpRequest'
    }

    def fetch_document_by_id(document):
        try:
            response = request_get(f'https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?id={document["id"]}&cvm=true&#toolbar=0', headers=headers)

            html_cropped_body = base64.b64decode(response.text).decode('utf-8')[1050:]

            return html_cropped_body
        except:
            log_error(f'Error fetching document from id {document["id"]} document for CNPJ {cnpj}: {traceback.format_exc()}')
            return None

    try:
        base_url = f'https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?d={document_configs["d"]}&s=0&l={document_configs.get("num_results", 10)}&o%5B0%5D%5BdataEntrega%5D=desc&tipoFundo=1&idCategoriaDocumento={document_configs["idCategoriaDocumento"]}&idTipoDocumento={document_configs["idTipoDocumento"]}&idEspecieDocumento=0&situacao=A&cnpj={cnpj}&cnpjFundo={cnpj}&isSession=false&_=1754204469153'

        day_month = document_configs.get('day_month')
        date_limitter_path = f'&dataInicial={day_month}%2F{document_configs["year"] -1}&dataFinal={day_month}%2F{document_configs["year"]}' if day_month else '&ultimaDataReferencia=true'

        response = request_get(f'{base_url}{date_limitter_path}', headers=headers)
        documents = response.json()

        final_documents = [ fetch_document_by_id(document) for document in documents['data'] ]

        return final_documents
    except:
        log_error(f'Error fetching all {document_configs["type"]} document for {cnpj}: {traceback.format_exc()}')
        return None

def _get_informe_mensal_estruturado_docs(cnpj):
    doc_configs = {
        'd': 3,
        'idCategoriaDocumento': 6,
        'idTipoDocumento': 40,
        'type': 'Informe Mensal Estruturado'
    }
    return _fetch_documents(cnpj, doc_configs)

def _get_informe_trimestral_estruturado_docs(cnpj):
    doc_configs = {
        'd': 4,
        'idCategoriaDocumento': 6,
        'idTipoDocumento': 45,
        'type': 'Informe Trimestral Estruturado'
    }
    return _fetch_documents(cnpj, doc_configs)

def _get_rendimentos_amortizacoes_docs(cnpj):
    today = datetime.now()
    today_day_month = f'{today.day if today.day >= 10 else f"0{today.day}"}%2F{today.month if today.month >= 10 else f"0{today.month}"}'

    doc_configs = {
        'd': 5,
        'day_month': today_day_month,
        'idCategoriaDocumento': 14,
        'idTipoDocumento': 41,
        'num_results': 25,
        'type': 'Rendimentos Amortizacoes',
        'year': today.year
    }

    RA_docs = _fetch_documents(cnpj, doc_configs)

    pattern_to_remove = '</td><td><span class="dado-valores">'

    simplified_RA_doc = { get_substring(doc, 'Data do pagamento', '</span>', pattern_to_remove): text_to_number(get_substring(doc, 'Valor do provento (R$/unidade)', '</span>', pattern_to_remove)) for doc in RA_docs }

    return simplified_RA_doc

def _get_cnpj_from_investidor10(ticker):
    global _investidor_10_preloaded_data

    patterns_to_remove = [ '<span>', '</span>', '<div class="value">' ]

    try:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'referer': 'https://investidor10.com.br/fiis/mxrf11/',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0'
        }

        response = request_get(f'https://investidor10.com.br/fiis/{ticker}', headers)
        html_cropped_body = response.text[15898:]

        cnpj = get_substring(html_cropped_body, 'CNPJ', '</div>', patterns_to_remove)

        if cnpj:
          _investidor_10_preloaded_data = (ticker, html_cropped_body)

        return cnpj
    except:
        _investidor_10_preloaded_data = (None, None)
        log_error(f'Error fetching CNPJ on Investidor 10 for "{ticker}": {traceback.format_exc()}')
        return None

def _get_cnpj_from_fiis(ticker):
    global _fiis_preloaded_data

    try:
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Origin': 'https://fiis.com.br',
            'Referer': 'https://fiis.com.br/lupa-de-fiis/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0'
        }

        response = request_get(f'https://fiis.com.br/{ticker}', headers=headers)
        html_page = response.text

        cnpj = get_substring(html_page, 'cnpj":"', '"', '\\')

        if cnpj:
          _fiis_preloaded_data = (ticker, html_page)

        return cnpj
    except:
        _fiis_preloaded_data = (None, None)
        log_error(f'Error fetching CNPJ on FIIs for "{ticker}": {traceback.format_exc()}')
        return None

def _get_cnpj_from_fundamentus(ticker):
    global _fundamentus_preloaded_data

    try:
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Origin': 'https://fundamentus.com.br/index.php',
            'Referer': 'https://fundamentus.com.br/index.php',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0'
        }

        response = request_get(f'https://fundamentus.com.br/detalhes.php?papel={ticker}', headers)
        html_page = response.text

        if 'Nenhum papel encontrado' in html_page:
            raise

        cnpj = get_substring(html_page, 'abrirGerenciadorDocumentosCVM?cnpjFundo=', '">Pesquisar Documentos', '#')

        if cnpj:
          _fundamentus_preloaded_data = (ticker, html_page)

        return cnpj
    except:
        _fundamentus_preloaded_data = (None, None)
        log_error(f'Error fetching CNPJ on Fundamentus for "{ticker}": {traceback.format_exc()}')
        return None

def _get_data_from_bmfbovespa(ticker, info_names):
    try:
        cnpj = (
            _get_cnpj_from_fundamentus(ticker) or
            _get_cnpj_from_fiis(ticker) or
            _get_cnpj_from_investidor10(ticker)
        )

        if not cnpj:
            log_error(f'No CNPJ found for "{ticker}"')
            return None

        informe_mensal_estruturado_docs = _get_informe_mensal_estruturado_docs(cnpj)
        informe_trimestral_estruturado_docs = _get_informe_trimestral_estruturado_docs(cnpj)
        rendimentos_amortizacoes_docs = _get_rendimentos_amortizacoes_docs(cnpj)

        converted_data = _convert_bmfbovespa_data(
            informe_mensal_estruturado_docs,
            informe_trimestral_estruturado_docs,
            rendimentos_amortizacoes_docs,
            cnpj,
            info_names
        )
        log_debug(f'Converted BM & FBovespa data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data on BM & FBovespa for "{ticker}": {traceback.format_exc()}')
        return None

def _convert_fundamentus_data(data, historical_prices, info_names):
    patterns_to_remove = [
        '</font>',
        '</span>',
        '</td>',
        '<a href="resultado.php?segmento=',
        '<font color="#306EFF">',
        '<font color="#F75D59">',
        '<span class="oscil">',
        '<span class="txt">',
        '<td class="data destaque w3">',
        '<td class="data w1">',
        '<td class="data w2">',
        '<td class="data w3">',
        '<td class="data">'
    ]

    prices = [ price[1] for price in historical_prices[-200:] ]
    avg_price = sum(prices) / len(prices)
    last_price = historical_prices[-1][1]

    def get_vacancy():
        vacancy_as_text = get_substring(data, 'Vacância Média</span>', '</span>', patterns_to_remove)
        vacancy_as_text = vacancy_as_text.replace('-', '').strip()
        return text_to_number(vacancy_as_text) if vacancy_as_text else None

    ALL_INFO = {
        'actuation': lambda: None,
        'assets_value': lambda: text_to_number(get_substring(data, '>Ativos</span>', '</span>', patterns_to_remove)),
        'avg_price': lambda: avg_price,
        'cash_value': lambda: text_to_number(get_substring(data, 'Caixa\'', ']', [', data : ['])),
        'debit_by_real_state_acquisition': lambda: None,
        'debit_by_securitization_receivables_acquisition': lambda: None,
        'dy': lambda: text_to_number(get_substring(data, 'Div. Yield</span>', '</span>', patterns_to_remove)),
        'equity_price': lambda: text_to_number(get_substring(data, 'VP/Cota</span>', '</span>', patterns_to_remove)),
        'ffoy': lambda: text_to_number(get_substring(data, 'FFO Yield</span>', '</span>', patterns_to_remove)),
        'initial_date': lambda: None,
        'latest_dividend': lambda: text_to_number(get_substring(data, 'Dividendo/cota</span>', '</span>', patterns_to_remove)),
        'latests_dividends': lambda: None,
        'link': lambda: get_substring(data, '<a target="_blank" href="', '">Pesquisar', '#'),
        'liquidity': lambda: text_to_number(get_substring(data, 'Vol $ méd (2m)</span>', '</span>', patterns_to_remove)),
        'management': lambda: get_substring(data, 'Gestão</span>', '</span>', patterns_to_remove),
        'market_value': lambda: text_to_number(get_substring(data, 'Valor de mercado</span>', '</span>', patterns_to_remove)),
        'max_52_weeks': lambda: text_to_number(get_substring(data, 'Max 52 sem</span>', '</span>', patterns_to_remove)),
        #'max_52_weeks': lambda: max(prices),
        'mayer_multiple': lambda: last_price / avg_price,
        'min_52_weeks': lambda: text_to_number(get_substring(data, 'Min 52 sem</span>', '</span>', patterns_to_remove)),
        #'min_52_weeks': lambda: min(prices),
        'name': lambda: get_substring(data, 'Nome</span>', '</span>', patterns_to_remove),
        'net_equity_value': lambda: text_to_number(get_substring(data, 'Patrim Líquido</span>', '</span>', patterns_to_remove)),
        'price': lambda: text_to_number(get_substring(data, 'Cotação</span>', '</span>', patterns_to_remove)),
        #'price': lambda: last_price,
        'pvp': lambda: text_to_number(get_substring(data, 'P/VP</span>', '</span>', patterns_to_remove)),
        'segment': lambda: get_substring(data, 'Mandato</span>', '</span>', patterns_to_remove),
        'target_public': lambda: None,
        'term': lambda: None,
        'total_issued_shares': lambda: text_to_number(get_substring(data, 'Nro. Cotas</span>', '</span>', patterns_to_remove)),
        'total_mortgage': lambda: None,
        'total_mortgage_value': lambda: None,
        'total_real_state': lambda: text_to_number(get_substring(data, 'Qtd imóveis</span>', '</span>', patterns_to_remove)),
        'total_real_state_value': lambda: None,
        'total_stocks_fund_others': lambda: None,
        'total_stocks_fund_others_value': lambda: None,
        'type': lambda: None,
        'vacancy': get_vacancy,
        'variation_12m': lambda: text_to_number(get_substring(data, '12 meses</span>', '</span>', patterns_to_remove)),
        'variation_30d': lambda: text_to_number(get_substring(data, 'Mês</span>', '</span>', patterns_to_remove))
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def _get_data_from_fundamentus(ticker, info_names):
    global _fundamentus_preloaded_data

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Origin': 'https://fundamentus.com.br/index.php',
        'Referer': 'https://fundamentus.com.br/index.php',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0'
    }

    def get_fundamentus_html_page():
        if _fundamentus_preloaded_data[1] and ticker == _fundamentus_preloaded_data[0]:
            log_debug(f'Using preloaded Fundamentus data')
            return _fundamentus_preloaded_data[1]

        response = request_get(f'https://fundamentus.com.br/detalhes.php?papel={ticker}', headers)
        html_page = response.text

        log_debug(f'Using fresh Fundamentus data')
        return html_page

    def get_fundamentus_historical_prices():
        response = request_get(f'https://www.fundamentus.com.br/amline/cot_hist.php?papel={ticker}', headers)
        historical_prices = response.json()
        return historical_prices

    try:
        converted_data = _convert_fundamentus_data(get_fundamentus_html_page(), get_fundamentus_historical_prices(), info_names)
        log_debug(f'Converted Fundamentus data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data on Fundamentus for "{ticker}": {traceback.format_exc()}')
        return None

def _convert_fiis_data(data, info_names):
    ALL_INFO = {
        'actuation': lambda: data['category'][0] if 'valor' in data['meta'] else None,
        'assets_value': lambda: None,
        'avg_price': lambda: None,
        'cash_value': lambda: data['meta']['valor_caixa'] if 'gestao' in data['meta'] else None,
        'debit_by_real_state_acquisition': lambda: None,
        'debit_by_securitization_receivables_acquisition': lambda: None,
        'dy': lambda: data['meta']['dy'] if 'dy' in data['meta'] else None,
        'equity_price': lambda: data['meta']['valorpatrimonialcota'] if 'valorpatrimonialcota' in data['meta'] else None,
        'ffoy': lambda: None,
        'initial_date': lambda: data['meta']['firstdate'] if 'firstdate' in data['meta'] else None,
        'latest_dividend': lambda: data['meta']['lastdividend'] if 'lastdividend' in data['meta'] else None,
        'latests_dividends': lambda: data['meta']['currentsumdividends'] if 'avgdividend' in data['meta'] else None,
        'link': lambda: f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={data["meta"]["cnpj"]}#',
        'liquidity': lambda: data['meta']['liquidezmediadiaria'] if 'liquidezmediadiaria' in data['meta'] else None,
        'management': lambda: data['meta']['gestao'] if 'valor_caixa' in data['meta'] else None,
        'market_value': lambda: data['meta']['valormercado'] if 'valormercado' in data['meta'] else None,
        'max_52_weeks': lambda: data['meta']['max_52_semanas'] if 'max_52_semanas' in data['meta'] else None,
        'mayer_multiple': lambda: None,
        'min_52_weeks': lambda: data['meta']['min_52_semanas'] if 'min_52_semanas' in data['meta'] else None,
        'name': lambda: data['meta']['name'] if 'name' in data['meta'] else None,
        'net_equity_value': lambda: data['meta']['patrimonio'] if 'patrimonio' in data['meta'] else None,
        'price': lambda: data['meta']['valor'] if 'valor' in data['meta'] else None,
        'pvp': lambda: data['meta']['pvp'] if 'pvp' in data['meta'] else None,
        'segment': lambda: data['meta']['segmento_ambima'] if 'segmento_ambima' in data['meta'] else None,
        'target_public': lambda: data['meta']['publicoalvo'] if 'publicoalvo' in data['meta'] else None,
        'term': lambda: data['meta']['prazoduracao'] if 'prazoduracao' in data['meta'] else None,
        'total_issued_shares': lambda: data['meta']['numero_cotas'] if 'numero_cotas' in data['meta'] else None,
        'total_mortgage': lambda: None,
        'total_mortgage_value': lambda: None,
        'total_real_state': lambda: data['meta']['assets_number'] if 'assets_number' in data['meta'] else None,
        'total_real_state_value': lambda: None,
        'total_stocks_fund_others': lambda: None,
        'total_stocks_fund_others_value': lambda: None,
        'type': lambda: data['meta']['setor_atuacao'] if 'setor_atuacao' in data['meta'] else None,
        'vacancy': lambda: data['meta']['vacancia'] if 'vacancia' in data['meta'] else None,
        'variation_12m': lambda: data['meta']['valorizacao_12_meses'] if 'valorizacao_12_meses' in data['meta'] else None,
        'variation_30d': lambda: data['meta']['valorizacao_mes'] if 'valorizacao_mes' in data['meta'] else None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def _get_data_from_fiis(ticker, info_names):
    global _fiis_preloaded_data

    try:
        if _fiis_preloaded_data[1] and ticker == _fiis_preloaded_data[0]:
            converted_data = _convert_fiis_data(_fiis_preloaded_data[1], info_names)
            log_debug(f'Converted preloaded FIIs data: {converted_data}')
            return converted_data

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Origin': 'https://fiis.com.br',
            'Referer': 'https://fiis.com.br/lupa-de-fiis/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0'
        }

        response = request_get(f'https://fiis.com.br/{ticker}', headers=headers)
        html_page = response.text

        raw_data = get_substring(html_page, 'var dataLayer_content', 'dataLayer.push')

        json_data = json.loads(raw_data.strip(';= '))['pagePostTerms']

        converted_data = _convert_fiis_data(json_data, info_names)
        log_debug(f'Converted fresh FIIs data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data on FIIs for "{ticker}": {traceback.format_exc()}')
        return None

def _convert_fundsexplorer_data(data, info_names):
    ALL_INFO = {
        'actuation': lambda: data['category'][0] if 'valor' in data['meta'] else None,
        'assets_value': lambda: None,
        'avg_price': lambda: None,
        'cash_value': lambda: data['meta']['valor_caixa'] if 'gestao' in data['meta'] else None,
        'debit_by_real_state_acquisition': lambda: None,
        'debit_by_securitization_receivables_acquisition': lambda: None,
        'dy': lambda: data['meta']['dy'] if 'dy' in data['meta'] else None,
        'equity_price': lambda: data['meta']['valorpatrimonialcota'] if 'valorpatrimonialcota' in data['meta'] else None,
        'ffoy': lambda: None,
        'initial_date': lambda: data['meta']['firstdate'] if 'firstdate' in data['meta'] else None,
        'latest_dividend': lambda: data['meta']['lastdividend'] if 'lastdividend' in data['meta'] else None,
        'latests_dividends': lambda: data['meta']['dividendos_12_meses'] if 'dividendos_12_meses' in data['meta'] else None,
        'link': lambda: f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={data["meta"]["cnpj"]}#',
        'liquidity': lambda: data['meta']['liquidezmediadiaria'] if 'liquidezmediadiaria' in data['meta'] else None,
        'management': lambda: data['meta']['gestao'] if 'valor_caixa' in data['meta'] else None,
        'market_value': lambda: data['meta']['valormercado'] if 'valormercado' in data['meta'] else None,
        'max_52_weeks': lambda: data['meta']['max_52_semanas'] if 'max_52_semanas' in data['meta'] else None,
        'mayer_multiple': lambda: None,
        'min_52_weeks': lambda: data['meta']['min_52_semanas'] if 'min_52_semanas' in data['meta'] else None,
        'name': lambda: data['meta']['name'] if 'name' in data['meta'] else None,
        'net_equity_value': lambda: data['meta']['patrimonio'] if 'patrimonio' in data['meta'] else None,
        'price': lambda: data['meta']['valor'] if 'valor' in data['meta'] else None,
        'pvp': lambda: data['meta']['pvp'] if 'pvp' in data['meta'] else None,
        'segment': lambda: data['meta']['segmento_ambima'] if 'segmento_ambima' in data['meta'] else None,
        'target_public': lambda: data['meta']['publicoalvo'] if 'publicoalvo' in data['meta'] else None,
        'term': lambda: data['meta']['prazoduracao'] if 'prazoduracao' in data['meta'] else None,
        'total_issued_shares': lambda: data['meta']['numero_cotas'] if 'numero_cotas' in data['meta'] else None,
        'total_mortgage_value': lambda: None,
        'total_mortgage': lambda: None,
        'total_real_state_value': lambda: None,
        'total_real_state': lambda: data['meta']['assets_number'] if 'assets_number' in data['meta'] else None,
        'total_stocks_fund_others_value': lambda: None,
        'total_stocks_fund_others': lambda: None,
        'type': lambda: data['meta']['setor_atuacao'] if 'setor_atuacao' in data['meta'] else None,
        'vacancy': lambda: data['meta']['vacancia'] if 'vacancia' in data['meta'] else None,
        'variation_12m': lambda: data['meta']['valorizacao_12_meses'] if 'valorizacao_12_meses' in data['meta'] else None,
        'variation_30d': lambda: data['meta']['valorizacao_mes'] if 'valorizacao_mes' in data['meta'] else None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def _get_data_from_fundsexplorer(ticker, info_names):
    try:
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'DNT': '1',
            'Priority': 'u=0, i',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0'
        }

        response = request_get(f'https://www.fundsexplorer.com.br/funds/{ticker}', headers)

        html_page = response.text
        raw_data = get_substring(html_page, 'var dataLayer_content', 'dataLayer.push')

        json_data = json.loads(raw_data.strip(';= '))['pagePostTerms']

        converted_data = _convert_fundsexplorer_data(json_data, info_names)
        log_debug(f'Converted Fundsexplorer: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data on Fundsexplorer for "{ticker}": {traceback.format_exc()}')
        return None

def _convert_investidor10_data(data, info_names):
    patterns_to_remove = [
        '</div>',
        '</span>',
        '<div class="_card-body">',
        '<div class="value">',
        '<div>',
        '<span class="content--info--item--value">',
        '<span class="value">',
        '<span>'
    ]

    count_pattern_on_text = lambda text, pattern: None if not text or not pattern else len(text.split(pattern))

    ALL_INFO = {
        'actuation': lambda: None,
        'assets_value': lambda: None,
        'avg_price': lambda: None,
        'cash_value': lambda: None,
        'debit_by_real_state_acquisition': lambda: None,
        'debit_by_securitization_receivables_acquisition': lambda: None,
        'dy': lambda: text_to_number(get_substring(data, 'DY (12M)</span>', '</span>', patterns_to_remove)),
        'equity_price': lambda: text_to_number(get_substring(data, 'VAL. PATRIMONIAL P/ COTA', '<div class=\'cell\'>', patterns_to_remove)),
        'ffoy': lambda: None,
        'initial_date': lambda: None,
        'latest_dividend': lambda: text_to_number(get_substring(data, 'ÚLTIMO RENDIMENTO', '</div>', patterns_to_remove)),
        'latests_dividends': lambda: text_to_number(get_substring(get_substring(data, 'YIELD 12 MESES', '</div>'), 'amount">', '</span>', patterns_to_remove)),
        'link': lambda: f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={get_substring(data, "CNPJ", "</div>", patterns_to_remove)}#',
        'liquidity': lambda: multiply_by_unit(get_substring(data, 'title="Liquidez Diária">Liquidez Diária</span>', '</span>', patterns_to_remove)),
        'management': lambda: get_substring(data, 'TIPO DE GESTÃO', '<div class=\'cell\'>', patterns_to_remove),
        'market_value': lambda: None,
        'max_52_weeks': lambda: None,
        'mayer_multiple': lambda: None,
        'min_52_weeks': lambda: None,
        'name': lambda: get_substring(data, 'Razão Social', '<div class=\'cell\'>', patterns_to_remove),
        'net_equity_value': lambda: multiply_by_unit(get_substring(data, 'VALOR PATRIMONIAL</span>', '</span>', patterns_to_remove)),
        'price': lambda: text_to_number(get_substring(data, 'Cotação</span>', '</span>', patterns_to_remove)),
        'pvp': lambda: text_to_number(get_substring(data, 'title="P/VP">P/VP</span>', '</span>', patterns_to_remove)),
        'segment': lambda: get_substring(data, 'SEGMENTO', '<div class=\'cell\'>', patterns_to_remove),
        'target_public': lambda: get_substring(data, 'PÚBLICO-ALVO', '<div class=\'cell\'>', patterns_to_remove),
        'term': lambda: get_substring(data, 'PRAZO DE DURAÇÃO', '<div class=\'cell\'>', patterns_to_remove),
        'total_issued_shares': lambda: text_to_number(get_substring(data, 'COTAS EMITIDAS', '<div class=\'cell\'>', patterns_to_remove)),
        'total_mortgage': lambda: None,
        'total_mortgage_value': lambda: None,
        'total_real_state': lambda: count_pattern_on_text(get_substring(data, 'Lista de Imóveis', '</section>'), 'card-propertie'),
        'total_real_state_value': lambda: None,
        'total_stocks_fund_others': lambda: None,
        'total_stocks_fund_others_value': lambda: None,
        'type': lambda: get_substring(data, 'TIPO DE FUNDO', '<div class=\'cell\'>', patterns_to_remove),
        'vacancy': lambda: text_to_number(get_substring(data, 'VACÂNCIA', '<div class=\'cell\'>', patterns_to_remove)),
        'variation_12m': lambda: text_to_number(get_substring(data, 'title="Variação (12M)">VARIAÇÃO (12M)</span>', '</span>', patterns_to_remove)),
        'variation_30d': lambda: text_to_number(get_substring(data, '>30</div>', '</div>'))
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def _get_data_from_investidor10(ticker, info_names):
    global _investidor_10_preloaded_data

    try:
        if _investidor_10_preloaded_data[1] and ticker == _investidor_10_preloaded_data[0]:
            converted_data = _convert_investidor10_data(_investidor_10_preloaded_data[1], info_names)
            log_debug(f'Converted preloaded Investidor 10 data: {converted_data}')
            return converted_data

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://investidor10.com.br/fiis/mxrf11/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0'
        }

        response = request_get(f'https://investidor10.com.br/fiis/{ticker}', headers)
        html_cropped_body = response.text[15898:]

        converted_data = _convert_investidor10_data(html_cropped_body, info_names)
        log_debug(f'Converted fresh Investidor 10 data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data on Investidor 10 for "{ticker}": {traceback.format_exc()}')
        return None

def _get_data_from_all_sources(ticker, info_names):
    data_bmfbovespa = _get_data_from_bmfbovespa(ticker, info_names)
    log_info(f'Data from BM & FBovespa: {data_bmfbovespa}')

    missing_bmfbovespa_infos = filter_remaining_infos(data_bmfbovespa, info_names)
    log_debug(f'Missing info from BM & FBovespa: {missing_bmfbovespa_infos}')

    if data_bmfbovespa and not missing_bmfbovespa_infos:
        return data_bmfbovespa

    data_fundamentus = _get_data_from_fundamentus(ticker, missing_bmfbovespa_infos or info_names)
    log_info(f'Data from Fundamentus: {data_fundamentus}')

    combined_data, missing_combined_infos = combine_data(data_bmfbovespa, data_fundamentus, info_names)
    log_debug(f'Missing info from BM & FBovespa or Fundamentus: {missing_combined_infos}')

    if combined_data and not missing_combined_infos:
        return combined_data

    data_fiis = _get_data_from_fiis(ticker, missing_combined_infos or info_names)
    log_info(f'Data from FIIs: {data_fiis}')

    combined_data, missing_combined_infos = combine_data(combined_data, data_fiis, info_names)
    log_debug(f'Missing info from BM & FBovespa, Fundamentus or FIIs: {missing_combined_infos}')

    if combined_data and not missing_combined_infos:
        return combined_data

    data_investidor_10 = _get_data_from_investidor10(ticker, missing_combined_infos or info_names)
    log_info(f'Data from Investidor 10: {data_investidor_10}')

    if not data_investidor_10:
        return combined_data

    return { **combined_data, **data_investidor_10 }

def _get_data_from_sources(ticker, source, info_names):
    SOURCES = {
        VALID_FII_SOURCES['BMFBOVESPA_SOURCE']: _get_data_from_bmfbovespa,
        VALID_FII_SOURCES['FIIS_SOURCE']: _get_data_from_fiis,
        VALID_FII_SOURCES['FUNDAMENTUS_SOURCE']: _get_data_from_fundamentus,
        VALID_FII_SOURCES['FUNDSEXPLORER_SOURCE']: _get_data_from_fundsexplorer,
        VALID_FII_SOURCES['INVESTIDOR10_SOURCE']: _get_data_from_investidor10
    }

    fetch_function = SOURCES.get(source, _get_data_from_all_sources)
    return fetch_function(ticker, info_names)

def get_fii_data(ticker, source, info_names, can_use_cache):
    cached_data = get_data_from_cache(ticker, CACHE_FILE_FII, info_names, can_use_cache)

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