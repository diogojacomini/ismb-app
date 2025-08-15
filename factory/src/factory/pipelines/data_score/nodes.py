"""
This is a boilerplate pipeline 'data_score' generated using Kedro 0.19.14
"""
import pandas as pd
from workalendar.america import Brazil
from datetime import date
import logging

logger = logging.getLogger(__name__)


def calculate_score_dim(config: dict,
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
    logger.info("Parameters - Odate: %s, Full Data: %s", odate, process_full_data)

    df = _generate_trading_days_calendar()
    df = df.merge(df_indicador_risco_credito[['dat_ref', 'score_risco_credito']], how='left', on='dat_ref')\
           .merge(df_indicador_retorno_mercado[['dat_ref', 'score_retorno_mercado']], how='left', on='dat_ref')\
           .merge(df_indicador_volatilidade_mercado[['dat_ref', 'score_volatilidade_mercado']], how='left', on='dat_ref')\
           .merge(df_indicador_atividade_mercado[['dat_ref', 'score_atividade_mercado']], how='left', on='dat_ref')\
           .merge(df_indicador_confianca_mercado_local[['dat_ref', 'score_confianca_mercado']], how='left', on='dat_ref')\
           .merge(df_indicador_sentimento_noticias[['dat_ref', 'score_noticias']], how='left', on='dat_ref')

    if not process_full_data:
        df = df[df["dat_ref"] == odate]

    metodo = config.get('metrica_calculo', 'ponderado')
    pesos = config.get('pesos', {})
    colunas = list(pesos.keys())

    logger.info("Aplicando score pelo método: %s", metodo)
    if metodo == "media":
        df["indice_isbm"] = df[colunas].mean(axis=1, skipna=True)

    elif metodo == "ponderado":
        df["indice_isbm"] = df.apply(lambda row: _calc_ponderado(row, pesos), axis=1)

    return df


def _generate_trading_days_calendar(start='2017-01-01'):
    """
    Gera um DataFrame com todos os dias úteis a partir de uma data inicial até a data atual.
    """
    cal = Brazil()
    years = range(pd.to_datetime(start).year, date.today().year + 1)
    feriados = [cal.holidays(y) for y in years]
    feriados = [dt for year in feriados for dt, _ in year]
    feriados = pd.to_datetime(feriados).strftime('%Y-%m-%d')

    # datas úteis (excluindo finais de semana)
    dates = pd.date_range(start=start, end=date.today(), freq='B')
    df = pd.DataFrame({'dat_ref': dates})
    df['dat_ref'] = df['dat_ref'].dt.strftime('%Y-%m-%d')

    return df[~df['dat_ref'].isin(feriados)]


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
