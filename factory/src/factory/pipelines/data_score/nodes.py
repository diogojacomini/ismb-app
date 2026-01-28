"""
This is a boilerplate pipeline 'data_score' generated using Kedro 0.19.14
"""
import pandas as pd
from workalendar.america import Brazil
from datetime import date
import logging

logger = logging.getLogger(__name__)


def calculate_score_dim(config: dict, dim_tempo: pd.DataFrame,
                        df_indicador_risco_credito: pd.DataFrame, df_indicador_retorno_mercado: pd.DataFrame,
                        df_indicador_volatilidade_mercado: pd.DataFrame, df_indicador_atividade_mercado: pd.DataFrame,
                        df_indicador_confianca_mercado_local: pd.DataFrame, df_indicador_sentimento_noticias: pd.DataFrame,
                        parameters: dict) -> pd.DataFrame:
    """
    Calcula o índice ISBM a partir dos indicadores.

    - score_risco_credito: Score de risco de crédito
    - score_retorno_mercado: Score de retorno do mercado
    - score_volatilidade_mercado: Score de volatilidade do mercado
    - score_atividade_mercado: Score de atividade do mercado
    - score_confianca_mercado: Score de confiança do mercado local
    - score_noticias: Score de sentimento de notícias
    - indice_isbm: Score final composto ISBM (0-100)
    """
    odate = parameters.get("odate")
    process_full_data = parameters.get("process_full_data", False)
    env = parameters.get("environment", 'prd')
    if env == 'test':
        return pd.DataFrame({"dat_ref": [odate],
                         "close_price": [999.99],
                         "open_price": [9999],
                         "high_price": [9999],
                         "low_price": [9999],
                         "change_percentage": [9999]
                         })

    logger.info("Parameters - Odate: %s, Full Data: %s", odate, process_full_data)

    df = dim_tempo[dim_tempo['dia_util'] == 1][['dat_ref', 'ano', 'sk_tempo']]
    
    if not process_full_data:
        df = df[df["dat_ref"] == odate]
    else:
        df = df[(df["ano"] >= 2018) & (df["sk_tempo"] <= int(odate.replace('-', '')))]

    df = df.merge(df_indicador_risco_credito[['dat_ref', 'score_risco_credito']], how='left', on='dat_ref')\
           .merge(df_indicador_retorno_mercado[['dat_ref', 'score_retorno_mercado']], how='inner', on='dat_ref')\
           .merge(df_indicador_volatilidade_mercado[['dat_ref', 'score_volatilidade_mercado']], how='inner', on='dat_ref')\
           .merge(df_indicador_atividade_mercado[['dat_ref', 'score_atividade_mercado']], how='inner', on='dat_ref')\
           .merge(df_indicador_confianca_mercado_local[['dat_ref', 'score_confianca_mercado']], how='inner', on='dat_ref')\
           .merge(df_indicador_sentimento_noticias[['dat_ref', 'score_noticias']], how='left', on='dat_ref')

    metodo = config.get('metrica_calculo', 'ponderado')
    pesos = config.get('pesos', {})
    colunas = list(pesos.keys())

    logger.info("Aplicando score pelo método: %s", metodo)
    if metodo == "media":
        df["indice_ismb"] = df[colunas].mean(axis=1, skipna=True)

    elif metodo == "ponderado":
        df["indice_ismb"] = df.apply(lambda row: _calc_ponderado(row, pesos), axis=1)

    return df[config.get('schema')]


def _calc_ponderado(row, pesos):
    """
    Calcula a média ponderada dos valores de uma linha do DataFrame.
    """
    total, peso_total = 0, 0
    for col, peso in pesos.items():
        if pd.notna(row.get(col)):
            total += row[col] * peso
            peso_total += peso
    return total / peso_total if peso_total > 0 else None
