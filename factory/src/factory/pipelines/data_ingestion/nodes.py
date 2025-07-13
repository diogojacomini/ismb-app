"""
This is a boilerplate pipeline 'data_ingestion'
generated using Kedro 0.19.14
"""
import pandas as pd
from .utils import scraping


def extract_transform_html_table(scraping_mapping: dict, columns_order: list) -> pd.DataFrame:
    """Função para extrair tabelas HTML."""
    if scraping_mapping.get('csv_read', False):
        return pd.read_csv(f"/home/diogo/society/data_master_cmp/ismb/backend/data/{scraping_mapping.get('csv_read')}.csv")

    data = scraping(scraping_mapping.get('url', None), scraping_mapping.get('headers', None))
    df = pd.DataFrame(data)

    return _transform_html_table(df, columns_order)


def _transform_html_table(raw_data: pd.DataFrame, columns_order: list) -> pd.DataFrame:
    """Transformação de dados html."""
    df = raw_data.copy()
    df.columns = columns_order
    # df = raw_data.rename(columns=columns_order)

    df['dat_ref'] = pd.to_datetime(df['dat_ref'], format='%b %d, %Y').dt.strftime('%Y-%m-%d')
    df = df.sort_values('dat_ref', ascending=False).reset_index(drop=True)
    df['change_percentage'] = df['change_percentage'].str.replace('%', '', regex=False)

    # Cast numérico
    columns_order.remove('dat_ref')

    for col in columns_order:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # TODO: filtro do dia
    return df
