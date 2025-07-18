"""
This is a boilerplate pipeline 'data_score'
generated using Kedro 0.19.14
"""
import pandas as pd
from workalendar.america import Brazil
from datetime import date


def calculate_score_dim(config: dict, df_indicador_risco_credito: pd.DataFrame, df_indicador_retorno_mercado: pd.DataFrame,
                        df_indicador_volatilidade_mercado: pd.DataFrame, df_indicador_atividade_mercado: pd.DataFrame,
                        df_indicador_confianca_mercado_local: pd.DataFrame, df_indicador_sentimento_noticias: pd.DataFrame) -> pd.DataFrame:

    df = _generate_trading_days_calendar()
    df = df.merge(df_indicador_risco_credito[['dat_ref', 'score_risco_credito']], how='left', on='dat_ref')\
           .merge(df_indicador_retorno_mercado[['dat_ref', 'score_retorno_mercado']], how='left', on='dat_ref')\
           .merge(df_indicador_volatilidade_mercado[['dat_ref', 'score_volatilidade_mercado']], how='left', on='dat_ref')\
           .merge(df_indicador_atividade_mercado[['dat_ref', 'score_atividade_mercado']], how='left', on='dat_ref')\
           .merge(df_indicador_confianca_mercado_local[['dat_ref', 'score_confianca_mercado']], how='left', on='dat_ref')\
           .merge(df_indicador_sentimento_noticias[['dat_ref', 'score_noticias']], how='left', on='dat_ref')

    metodo = config.get('metrica_calculo', 'ponderado')
    pesos = config.get('pesos', {})
    colunas = list(pesos.keys())

    if metodo == "media":
        df["indice_isbm"] = df[colunas].mean(axis=1, skipna=True)

    elif metodo == "ponderado":
        df["indice_isbm"] = df.apply(lambda row: _calc_ponderado(row, pesos), axis=1)

    return df  # [["dat_ref", "indice_isbm"]]


def _generate_trading_days_calendar(start='2000-01-01'):
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
    total, peso_total = 0, 0
    for col, peso in pesos.items():
        if pd.notna(row.get(col)):
            total += row[col] * peso
            peso_total += peso
    return total / peso_total if peso_total > 0 else None
