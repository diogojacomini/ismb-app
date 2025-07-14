"""
This is a boilerplate pipeline 'data_ingestion'
generated using Kedro 0.19.14
"""
import pandas as pd
import yfinance as yf
from .utils import scraping, scraping_infomoney, scraping_valorinveste, scraping_seudinheiro, scraping_moneytimes
from .utils import extrair_campos, extrair_data_url, parse_data_portugues, data_relativa_para_absoluta, select_cast_midia
from typing import Dict


def extract_transform_html_table(scraping_mapping: dict, columns_order: list) -> pd.DataFrame:
    """Função para extrair tabelas HTML."""
    if scraping_mapping.get('csv_read', False):
        return pd.read_csv(f"/home/diogo/society/data_master_cmp/ismb-app/factory/data/sandbox/{scraping_mapping.get('csv_read')}.csv")

    data = scraping(scraping_mapping.get('url', None), scraping_mapping.get('headers', None))
    df = pd.DataFrame(data)

    return _transform_html_table(df, columns_order, scraping_mapping.get('dat_ref_format'))


def _transform_html_table(raw_data: pd.DataFrame, columns_order: list, dat_format: str) -> pd.DataFrame:
    """Transformação de dados html."""
    df = raw_data.copy()
    if len(df.columns) == 7:
        df.drop(columns=5, inplace=True)

    df.columns = columns_order
    df['dat_ref'] = pd.to_datetime(df['dat_ref'], format=dat_format).dt.strftime('%Y-%m-%d')
    df = df.sort_values('dat_ref', ascending=False).reset_index(drop=True)
    df['change_percentage'] = df['change_percentage'].str.replace('%', '', regex=False)

    # Cast numérico
    columns_order.remove('dat_ref')

    for col in columns_order:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # TODO: filtro do dia
    return df


def extract_transform_api_yf(ticker: str, columns_mapping: Dict[str, str]) -> pd.DataFrame:
    """Extrai e transforma dados de ações usando a API do yfinance."""
    df = yf.download(ticker, start="2023-01-01")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    df = df.reset_index(inplace=False)
    df = df.rename(columns=columns_mapping)

    # TODO: filtro do dia
    return df


def extract_transform_infomoney(columns_order: Dict[str, str]) -> pd.DataFrame:
    """Extrai e transforma dados do InfoMoney."""
    df = pd.DataFrame(scraping_infomoney(columns_order.get('url'), columns_order.get('class_')))

    df[['categoria', 'titulo', 'data_publicacao']] = df['titulo'].apply(extrair_campos)
    df['dat_ref'] = pd.to_datetime(df['dat_ref']).dt.strftime("%Y-%m-%d")
    df = df[df['categoria'] != 'Esportes']
    df = select_cast_midia(df)

    # TODO: filtro do dia
    return df


def extract_transform_valorinveste(columns_order: Dict[str, str]) -> pd.DataFrame:
    """Extrai e transforma dados do Valor Investe."""
    df = pd.DataFrame(scraping_valorinveste(columns_order.get('url'),
                                            columns_order.get('class_post'),
                                            columns_order.get('class_date')))

    df['dat_ref'] = df['link'].apply(extrair_data_url)
    df = select_cast_midia(df)
    df['dat_ref'] = pd.to_datetime(df['dat_ref'], format='%Y/%m/%d').dt.strftime('%Y-%m-%d')

    # TODO: filtro do dia
    return df


def extract_transform_seudinheiro(columns_order: Dict[str, str]) -> pd.DataFrame:
    """Extrai e transforma dados do Seu Dinheiro."""
    df = pd.DataFrame(scraping_seudinheiro(columns_order.get('url'),
                                           columns_order.get('class_feed'),
                                           columns_order.get('class_title'),
                                           columns_order.get('class_date')))

    df['dat_ref'] = df['dat_ref'].apply(parse_data_portugues)
    df = select_cast_midia(df)

    # TODO: filtro do dia
    return df


def extract_transform_moneytimes(columns_order: Dict[str, str]) -> pd.DataFrame:
    """Extrai e transforma dados do MoneyTimes."""
    df = pd.DataFrame(scraping_moneytimes(columns_order.get('url'),
                                          columns_order.get('class_item'),
                                          columns_order.get('class_title'),
                                          columns_order.get('class_date')))

    df['dat_ref'] = df['dat_ref'].apply(data_relativa_para_absoluta)
    df = select_cast_midia(df)

    # TODO: filtro do dia
    return df
