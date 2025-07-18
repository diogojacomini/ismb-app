"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.14
"""
from .utils import ewma_volatility, normalizar_escala, analisar_sentimento
import pandas as pd
import numpy as np


def indicador_risco_credito(df, window=30, variacia=21):
    df = df.sort_values('dat_ref', ascending=True)
    df = ewma_volatility(df, variacia, lambda_=0.94)
    df['retorno_diario'] = df['close_price'].pct_change(periods=window)

    df = df.dropna(subset=['vol_ewma', 'retorno_diario'])
    df['rank_vol'] = df['vol_ewma'].rank(pct=True)
    df['rank_retorno'] = (-df['retorno_diario']).rank(pct=True)

    w_vol = 0.5
    w_retorno = 0.5

    # indicador Composto
    df['risco_bruto'] = (w_vol * df['rank_vol']) + (w_retorno * df['rank_retorno'])

    # normalização 0-100 (0 = medo extremo, 100 = ganância extrema)
    df['score_risco_credito'] = (1 - df['risco_bruto']) * 100

    return df[['dat_ref', 'retorno_diario', 'vol_ewma', 'rank_vol', 'rank_retorno', 'risco_bruto', 'score_risco_credito']]


def indicador_retorno_mercado(df_ibov: pd.DataFrame) -> pd.DataFrame:
    df_ibov = df_ibov.sort_values('dat_ref', ascending=True)

    #  retorno logarítmico
    df_ibov['log_ret'] = np.log(df_ibov['close'] / df_ibov['close'].shift(1))

    # média e desvio padrão móvel (21 dias úteis)
    df_ibov['media_ret'] = df_ibov['log_ret'].rolling(window=21).mean()
    df_ibov['desvio_ret'] = df_ibov['log_ret'].rolling(window=21).std()

    # Z-score do retorno
    df_ibov['z_retorno'] = (df_ibov['log_ret'] - df_ibov['media_ret']) / df_ibov['desvio_ret']

    # normalizar z-score (0–100)
    df_ibov['score'] = normalizar_escala(df_ibov['z_retorno'])

    # ponderar por volume (volume do dia / média 30 dias)
    df_ibov['media_vol'] = df_ibov['volume'].rolling(30).mean()
    fator_vol = (df_ibov['volume'] / df_ibov['media_vol']).clip(lower=0.7)
    df_ibov['score_ponderado'] = df_ibov['score'] * fator_vol

    df_ibov['score_retorno_mercado'] = normalizar_escala(df_ibov['score_ponderado'])
    return df_ibov[['dat_ref', 'log_ret', 'media_ret', 'desvio_ret', 'z_retorno', 'media_vol', 'score_retorno_mercado']]


def indicador_volatilidade_mercado(df_ibov: pd.DataFrame, df_ivvb: pd.DataFrame) -> pd.DataFrame:
    df_ibov = df_ibov.sort_values('dat_ref', ascending=True)
    df_ivvb = df_ivvb.sort_values('dat_ref', ascending=True)

    #  retorno logarítmico
    df_ibov['log_ret'] = np.log(df_ibov['close'] / df_ibov['close'].shift(1))

    # EWMA (volatilidade histórica com alpha ~0.06)
    alpha = 0.94
    df_ibov['ewma_var'] = df_ibov['log_ret'].ewm(alpha=1 - alpha).var()
    df_ibov['ewma_vol'] = np.sqrt(df_ibov['ewma_var']) * np.sqrt(252) * 100

    # ATR (volatilidade intradiária)
    high_low = df_ibov['high'] - df_ibov['low']
    high_close = np.abs(df_ibov['high'] - df_ibov['close'].shift(1))
    low_close = np.abs(df_ibov['low'] - df_ibov['close'].shift(1))
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df_ibov['atr'] = tr.rolling(window=14).mean()

    # IVVB11 (proxy de percepção global / VIX Brasil)
    df_ivvb['retorno_ivvb'] = df_ivvb['close'].pct_change()

    # Volatilidade histórica do IVVB11 (EWMA de retornos)
    df_ivvb['vol_ivvb'] = df_ivvb['retorno_ivvb'].ewm(alpha=0.06).std() * np.sqrt(252) * 100

    # Normalização dos scores
    df_ibov['score_ewma'] = normalizar_escala(df_ibov['ewma_vol'])
    df_ibov['score_atr'] = normalizar_escala(df_ibov['atr'])
    df_ivvb['score_ivvb'] = normalizar_escala(df_ivvb['vol_ivvb'])

    df_score = pd.merge(df_ibov[['dat_ref', 'log_ret', 'ewma_var', 'ewma_vol', 'atr', 'score_ewma', 'score_atr']],
                        df_ivvb[['dat_ref', 'retorno_ivvb', 'vol_ivvb', 'score_ivvb']], on='dat_ref', how='inner')

    df_score = df_score.dropna()

    # Score final ponderado
    df_score['score_volatilidade_mercado'] = 100 - ( # TODO: parametrizar os pesos
        df_score['score_ewma'] * 0.5 +
        df_score['score_atr'] * 0.3 +
        df_score['score_ivvb'] * 0.2
    )
    return df_score


def indicador_atividade_mercado(df_ibov: pd.DataFrame) -> pd.DataFrame:
    df_ibov = df_ibov.sort_values('dat_ref', ascending=True)

    # retorno diário
    df_ibov['retorno'] = df_ibov['close'].pct_change() * 100

    # desvio em relação à média móvel (3 dias)
    mm3 = df_ibov['retorno'].rolling(3).mean()
    df_ibov['desvio_relativo'] = (df_ibov['retorno'] / mm3) - 1

    # normalizar o score de desvio entre 0–100 com base nos percentis históricos
    df_ibov['score'] = normalizar_escala(df_ibov['desvio_relativo'])

    # ponderar score com volume (volume do dia / média dos últimos 30 dias)
    media_vol = df_ibov['volume'].rolling(30).mean()
    fator_vol = (df_ibov['volume'] / media_vol).clip(lower=0.7)
    df_ibov['score_atividade_mercado'] = df_ibov['score'] * fator_vol
    df_ibov['score_atividade_mercado'] = normalizar_escala(df_ibov['score_atividade_mercado'])

    return df_ibov[['dat_ref', 'retorno', 'desvio_relativo', 'score', 'score_atividade_mercado']]


def indicador_confianca_mercado_local(df_ifix: pd.DataFrame) -> pd.DataFrame:
    df_ifix['retorno'] = df_ifix['close_price'].pct_change() * 100

    # calcular média móvel de 3 dias no retorno
    df_ifix['retorno_mm3'] = df_ifix['retorno'].rolling(3).mean() # TPDP:

    # desvio relativo (se retorno está acima ou abaixo da média recente)
    df_ifix['desvio_relativo'] = (df_ifix['retorno'] / df_ifix['retorno_mm3']) - 1

    # normalizar (robusto: percentis 5% e 95%)
    df_ifix['score_confianca_mercado'] = normalizar_escala(df_ifix['desvio_relativo'])

    return df_ifix[['dat_ref', 'retorno', 'retorno_mm3', 'desvio_relativo', 'score_confianca_mercado']]


def indicador_sentimento_midia(df_rw_infomoney, df_rw_moneytimes, df_rw_seudinheiro, df_rw_valorinveste):
    df = pd.concat([df_rw_infomoney, df_rw_moneytimes, df_rw_seudinheiro, df_rw_valorinveste], ignore_index=True)
    sentimentos = df["titulo"].apply(analisar_sentimento)
    df_sent = pd.DataFrame(sentimentos.tolist())
    df = pd.concat([df, df_sent], axis=1)

    # score final bruto
    df["score_sentimento"] = (df["positivo"] - df["negativo"]) * 100 / (df[["positivo", "negativo"]].sum(axis=1) + 1e-5)

    # normalizar para 0–100 (percentis robustos)
    df['score_noticias'] = normalizar_escala(df['score_sentimento'])
    sentimento_dia = df.groupby(df['dat_ref'])['score_noticias'].mean().reset_index()
    return sentimento_dia
