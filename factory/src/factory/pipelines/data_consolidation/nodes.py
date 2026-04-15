"""
This is a boilerplate pipeline 'data_consolidation' generated using Kedro 0.19.14
"""
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def consolidate_data_transacoes(
    stage_cds: pd.DataFrame,
    stage_ibov: pd.DataFrame,
    stage_ivvb: pd.DataFrame,
    stage_ifix: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Une os datasets de mercado (CDS, IBOV, IVVB, IFIX) em um unico DataFrame.

    Filtra por odate quando process_full_data for False.
    Adiciona a coluna cod_indice para identificar a origem.
    """
    # Parametros
    odate = parameters.get("odate")
    process_full_data = parameters.get("process_full_data", False)

    sources = [
        (stage_cds, "CDS"),
        (stage_ibov, "IBOV"),
        (stage_ivvb, "IVVB"),
        (stage_ifix, "IFIX"),
    ]

    consolidated_parts = []
    for df, cod_indice in sources:
        if not process_full_data:
            df = df[df["dat_ref"] == odate]

        processed_df = _select_columns_transacoes(df, cod_indice)
        consolidated_parts.append(processed_df)

    return pd.concat(
        consolidated_parts,
        ignore_index=True,
    )


def consolidate_data_noticias(
    stage_infomoney: pd.DataFrame,
    stage_valorinveste: pd.DataFrame,
    stage_seudinheiro: pd.DataFrame,
    stage_moneytimes: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Une os datasets de noticias das quatro fontes em um unico DataFrame.

    Filtra por janela de 3 dias anteriores ao odate quando process_full_data for False.
    Adiciona a coluna cod_fonte para identificar a origem.
    """
    odate = parameters.get("odate")
    process_full_data = parameters.get("process_full_data", False)

    sources = [
        (stage_infomoney, "INFOMONEY"),
        (stage_valorinveste, "VALORINVEST"),
        (stage_seudinheiro, "SEUDINHEIRO"),
        (stage_moneytimes, "MONEYTIMES"),
    ]

    consolidated_parts = []
    for df, cod_fonte in sources:
        if not process_full_data:
            data_limite = (pd.to_datetime(odate) - pd.Timedelta(days=3)).strftime("%Y-%m-%d")
            df = df[(df["dat_ref"] >= data_limite) & (df["dat_ref"] <= odate)]

        processed_df = _select_columns_news(df, cod_fonte)
        consolidated_parts.append(processed_df)

    return pd.concat(
        consolidated_parts,
        ignore_index=True,
    )


def _select_columns_transacoes(df: pd.DataFrame, cod_indice: str) -> pd.DataFrame:
    """Renomeia colunas de preco para o padrao curated e adiciona cod_indice."""
    column_mapping = {
        "open_price": "val_abertura",
        "close_price": "val_fechamento",
        "high_price": "val_maxima",
        "low_price": "val_minima",
        "volume": "qtd_volume",
    }

    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

    # cod indice
    df["cod_indice"] = cod_indice

    # Garantir colunas obrigatórias
    required_cols = {
        "qtd_volume": np.nan,
    }
    for col, default in required_cols.items():
        if col not in df.columns:
            df[col] = default

    return df[
        [
            "dat_ref",
            "cod_indice",
            "val_fechamento",
            "val_abertura",
            "val_maxima",
            "val_minima",
            "qtd_volume",
        ]
    ]


def _select_columns_news(df: pd.DataFrame, cod_fonte: str) -> pd.DataFrame:
    """Renomeia colunas de noticias para o padrao curated e adiciona cod_fonte."""
    column_mapping = {
        "titulo": "txt_titulo",
        "id_news": "id_noticia",
    }

    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

    # cod fonte
    df["cod_fonte"] = cod_fonte

    return df[
        [
            "id_noticia",
            "dat_ref",
            "cod_fonte",
            "txt_titulo"
        ]
    ]
