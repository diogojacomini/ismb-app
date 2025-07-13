"""
This is a boilerplate pipeline 'data_ingestion'
generated using Kedro 0.19.14
"""
import pandas as pd
import yfinance as yf
from .utils import scraping, scraping_infomoney
from .utils import extrair_campos
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
    df = pd.DataFrame(scraping_infomoney(columns_order.get('url'), columns_order.get('class_')))

    df[['categoria', 'titulo', 'data_publicacao']] = df['titulo'].apply(extrair_campos)
    df['dat_ref'] = pd.to_datetime(df['dat_ref']).dt.strftime("%Y-%m-%d")
    df = df[df['categoria'] != 'Esportes']

    df = df[['dat_ref', 'fonte', 'titulo', 'link']]
    df = df.astype({col: 'string' for col in df.columns if col != 'dat_ref'})

    # TODO: filtro do dia
    return df
