"""
This is a boilerplate pipeline 'data_processing' generated using Kedro 0.19.14
"""
import pandas as pd
import numpy as np
from .utils import (
    ewma_volatility,
    normalizar_escala,
    analisar_sentimento,
    logger
)


def indicador_risco_credito(df, parms_indicador: dict, parameters: dict = None) -> pd.DataFrame:
    """
    Calcula indicador de risco de crédito baseado em volatilidade e retorno.

    Combina volatilidade EWMA e retorno diário para gerar um score de risco normalizado de 0 a 100,
    onde 0 representa medo extremo e 100 ganância extrema.

    - retorno_diario: Retorno percentual diário
    - vol_ewma: Volatilidade EWMA
    - rank_vol: Ranking percentual da volatilidade
    - rank_retorno: Ranking percentual do retorno (invertido)
    - risco_bruto: Score de risco bruto (0-1)
    - score_risco_credito: Score final de risco (0-100)
    """
    odate = parameters.get("odate")
    process_full_data = parameters.get("process_full_data", False)
    logger.info("Parameters - Odate: %s, Full Data: %s", odate, process_full_data)

    df['dat_ref'] = pd.to_datetime(df['dat_ref'], errors='coerce').dt.strftime('%Y-%m-%d')
    df = df.sort_values('dat_ref', ascending=True)
    if not process_full_data:
        lookback_days = max(int(parms_indicador.get("window", 30) * 1.4 * 6), 252)  # 252 = dias úteis em 1 ano
        data_limite = (pd.to_datetime(odate) - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        logger.info("Data limite: %s (lookback_days=%d)", data_limite, lookback_days)

        df = df[(df['dat_ref'] >= data_limite) & (df['dat_ref'] <= odate)]

    df = ewma_volatility(df, parms_indicador.get("variacia", 21), lambda_=parms_indicador.get("lambda_ewma", 0.94))
    df['retorno_diario'] = df['close_price'].pct_change(periods=parms_indicador.get("window", 30))

    df = df.dropna(subset=['vol_ewma', 'retorno_diario'])
    df['rank_vol'] = df['vol_ewma'].rank(pct=True)
    df['rank_retorno'] = (-df['retorno_diario']).rank(pct=True)

    # indicador Composto
    df['risco_bruto'] = (parms_indicador.get("w_vol", 0.5) * df['rank_vol']) + (parms_indicador.get("w_retorno", 0.5) * df['rank_retorno'])

    # normalização 0-100 (0 = medo extremo, 100 = ganância extrema)
    df['score_risco_credito'] = (1 - df['risco_bruto']) * 100

    if not process_full_data:
        df = df[df["dat_ref"] == odate]

    return df[['dat_ref', 'retorno_diario', 'vol_ewma', 'rank_vol', 'rank_retorno', 'risco_bruto', 'score_risco_credito']]


def indicador_retorno_mercado(df_ibov: pd.DataFrame, parms_indicador: dict, parameters: dict) -> pd.DataFrame:
    """
    Calcula indicador de retorno do mercado baseado no Ibovespa.

    Utiliza retorno logarítmico, Z-score normalizado e ponderação por volume
    para gerar um indicador de performance do mercado.

    - log_ret: Retorno logarítmico
    - media_ret: Média móvel do retorno (21 dias)
    - desvio_ret: Desvio padrão móvel (21 dias)
    - z_retorno: Z-score do retorno
    - media_vol: Média móvel do volume (30 dias)
    - score_retorno_mercado: Score final normalizado (0-100)
    """
    odate = parameters.get("odate")
    process_full_data = parameters.get("process_full_data", False)
    logger.info("Parameters - Odate: %s, Full Data: %s", odate, process_full_data)

    df_ibov['dat_ref'] = pd.to_datetime(df_ibov['dat_ref'], errors='coerce').dt.strftime('%Y-%m-%d')
    df_ibov = df_ibov.sort_values('dat_ref', ascending=True)

    if not process_full_data:
        lookback_days = max(65, parms_indicador.get("lookback_days", 180))  # 65 dias para médias + buffer para normalização
        data_limite = (pd.to_datetime(odate) - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        logger.info("Data limite: %s (lookback_days=%d)", data_limite, lookback_days)

        df_ibov = df_ibov[(df_ibov['dat_ref'] >= data_limite) & (df_ibov['dat_ref'] <= odate)]

    #  retorno logarítmico
    df_ibov['log_ret'] = np.log(df_ibov['close'] / df_ibov['close'].shift(1))

    # média e desvio padrão móvel (21 dias úteis)
    df_ibov['media_ret'] = df_ibov['log_ret'].rolling(window=parms_indicador.get('rolling_mean_window', 21)).mean()
    df_ibov['desvio_ret'] = df_ibov['log_ret'].rolling(window=parms_indicador.get('rolling_std_window', 21)).std()

    # Z-score do retorno
    df_ibov['z_retorno'] = (df_ibov['log_ret'] - df_ibov['media_ret']) / df_ibov['desvio_ret']

    # normalizar z-score (0–100)
    df_ibov['score'] = normalizar_escala(df_ibov['z_retorno'])

    # ponderar por volume (volume do dia / média 30 dias)
    df_ibov['media_vol'] = df_ibov['volume'].rolling(parms_indicador.get('volume_mean_window', 30)).mean()
    fator_vol = (df_ibov['volume'] / df_ibov['media_vol']).clip(lower=parms_indicador.get('volume_clip', 0.7))
    df_ibov['score_ponderado'] = df_ibov['score'] * fator_vol

    df_ibov['score_retorno_mercado'] = normalizar_escala(df_ibov['score_ponderado'])

    if not process_full_data:
        df_ibov = df_ibov[df_ibov["dat_ref"] == odate]

    return df_ibov[['dat_ref', 'log_ret', 'media_ret', 'desvio_ret', 'z_retorno', 'media_vol', 'score_retorno_mercado']]


def indicador_volatilidade_mercado(df_ibov: pd.DataFrame, df_ivvb: pd.DataFrame, parms_indicador: dict, parameters: dict) -> pd.DataFrame:
    """
    Calcula indicador de volatilidade do mercado.

    Combina volatilidade EWMA do Ibovespa, ATR (Average True Range) e volatilidade
    do IVVB11 para criar um indicador composto de volatilidade do mercado.

    - log_ret: Retorno logarítmico do Ibovespa
    - ewma_var: Variância EWMA
    - ewma_vol: Volatilidade EWMA anualizada
    - atr: Average True Range
    - score_ewma: Score normalizado da volatilidade EWMA
    - score_atr: Score normalizado do ATR
    - retorno_ivvb: Retorno do IVVB11
    - vol_ivvb: Volatilidade do IVVB11
    - score_ivvb: Score normalizado da volatilidade IVVB11
    - score_volatilidade_mercado: Score final ponderado (0-100)
    """
    odate = parameters.get("odate")
    process_full_data = parameters.get("process_full_data", False)
    logger.info("Parameters - Odate: %s, Full Data: %s", odate, process_full_data)

    df_ibov['dat_ref'] = pd.to_datetime(df_ibov['dat_ref'], errors='coerce').dt.strftime('%Y-%m-%d')
    df_ivvb['dat_ref'] = pd.to_datetime(df_ivvb['dat_ref'], errors='coerce').dt.strftime('%Y-%m-%d')
    df_ibov = df_ibov.sort_values('dat_ref', ascending=True)
    df_ivvb = df_ivvb.sort_values('dat_ref', ascending=True)

    if not process_full_data:
        lookback_days = max(65, parms_indicador.get("lookback_days", 180))  # 65 dias para cálculos + buffer para normalização
        data_limite = (pd.to_datetime(odate) - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        logger.info("Data limite: %s (lookback_days=%d)", data_limite, lookback_days)

        df_ibov = df_ibov[(df_ibov['dat_ref'] >= data_limite) & (df_ibov['dat_ref'] <= odate)]
        df_ivvb = df_ivvb[(df_ivvb['dat_ref'] >= data_limite) & (df_ivvb['dat_ref'] <= odate)]

    #  retorno logarítmico
    df_ibov['log_ret'] = np.log(df_ibov['close'] / df_ibov['close'].shift(1))

    # EWMA (volatilidade histórica com alpha ~0.06)
    df_ibov['ewma_var'] = df_ibov['log_ret'].ewm(alpha=1 - parms_indicador.get('alpha_ewma', 0.94)).var()
    df_ibov['ewma_vol'] = np.sqrt(df_ibov['ewma_var']) * np.sqrt(252) * 100

    # ATR (volatilidade intradiária)
    high_low = df_ibov['high'] - df_ibov['low']
    high_close = np.abs(df_ibov['high'] - df_ibov['close'].shift(1))
    low_close = np.abs(df_ibov['low'] - df_ibov['close'].shift(1))
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df_ibov['atr'] = tr.rolling(window=parms_indicador.get('atr_window', 14)).mean()

    # IVVB11 (proxy de percepção global / VIX Brasil)
    df_ivvb['retorno_ivvb'] = df_ivvb['close'].pct_change()

    # Volatilidade histórica do IVVB11 (EWMA de retornos)
    df_ivvb['vol_ivvb'] = df_ivvb['retorno_ivvb'].ewm(alpha=parms_indicador.get('alpha_ivvb', 0.06)).std() * np.sqrt(252) * 100

    # Normalização dos scores
    df_ibov['score_ewma'] = normalizar_escala(df_ibov['ewma_vol'])
    df_ibov['score_atr'] = normalizar_escala(df_ibov['atr'])
    df_ivvb['score_ivvb'] = normalizar_escala(df_ivvb['vol_ivvb'])

    df_score = pd.merge(df_ibov[['dat_ref', 'log_ret', 'ewma_var', 'ewma_vol', 'atr', 'score_ewma', 'score_atr']],
                        df_ivvb[['dat_ref', 'retorno_ivvb', 'vol_ivvb', 'score_ivvb']], on='dat_ref', how='inner')

    df_score = df_score.dropna()

    # Score final ponderado
    df_score['score_volatilidade_mercado'] = 100 - (
        df_score['score_ewma'] * parms_indicador.get('ewma_weight', 0.5) +
        df_score['score_atr'] * parms_indicador.get('atr_weight', 0.3) +
        df_score['score_ivvb'] * parms_indicador.get('ivvb_weight', 0.2)
    )

    if not process_full_data:
        df_score = df_score[df_score["dat_ref"] == odate]

    return df_score


def indicador_atividade_mercado(df_ibov: pd.DataFrame, parms_indicador: dict, parameters: dict) -> pd.DataFrame:
    """
    Calcula indicador de atividade do mercado baseado em retorno e volume.

    Analisa o desvio do retorno diário em relação a média móvel de 3 dias,
    ponderado pelo volume relativo para capturar a atividade do mercado.

    - retorno: Retorno diário em percentual
    - desvio_relativo: Desvio em relação a média móvel de 3 dias
    - score: Score normalizado do desvio
    - score_atividade_mercado: Score final ponderado por volume (0-100)
    """
    odate = parameters.get("odate")
    process_full_data = parameters.get("process_full_data", False)
    logger.info("Parameters - Odate: %s, Full Data: %s", odate, process_full_data)

    df_ibov['dat_ref'] = pd.to_datetime(df_ibov['dat_ref'], errors='coerce').dt.strftime('%Y-%m-%d')
    df_ibov = df_ibov.sort_values('dat_ref', ascending=True)

    if not process_full_data:
        lookback_days = max(3, parms_indicador.get("volume_mean_window", 180))
        data_limite = (pd.to_datetime(odate) - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        logger.info("Data limite: %s (lookback_days=%d)", data_limite, lookback_days)

        df_ibov = df_ibov[(df_ibov['dat_ref'] >= data_limite) & (df_ibov['dat_ref'] <= odate)]

    # retorno diário
    df_ibov['retorno'] = df_ibov['close'].pct_change() * 100

    # desvio em relação a média móvel (3 dias)
    mm3 = df_ibov['retorno'].rolling(parms_indicador.get('rolling_return_window', 3)).mean()
    df_ibov['desvio_relativo'] = (df_ibov['retorno'] / mm3) - 1

    # normalizar o score de desvio entre 0–100 com base nos percentis históricos
    df_ibov['score'] = normalizar_escala(df_ibov['desvio_relativo'])

    # ponderar score com volume (volume do dia / média dos últimos 30 dias)
    media_vol = df_ibov['volume'].rolling(parms_indicador.get('volume_mean_window', 30)).mean()
    fator_vol = (df_ibov['volume'] / media_vol).clip(lower=parms_indicador.get('volume_clip', 0.7))
    df_ibov['score_atividade_mercado'] = df_ibov['score'] * fator_vol
    df_ibov['score_atividade_mercado'] = normalizar_escala(df_ibov['score_atividade_mercado'])

    if not process_full_data:
        df_ibov = df_ibov[df_ibov["dat_ref"] == odate]

    return df_ibov[['dat_ref', 'retorno', 'desvio_relativo', 'score', 'score_atividade_mercado']]


def indicador_confianca_mercado_local(df_ifix: pd.DataFrame, parms_indicador: dict, parameters: dict) -> pd.DataFrame:
    """
    Calcula indicador de confiança do mercado local baseado no IFIX.

    Utiliza o índice de fundos imobiliários (IFIX) para confiança no mercado
    local, analisando desvios do retorno em relação a média recente.

    - retorno: Retorno diário em percentual
    - retorno_mm: Média móvel do retorno
    - desvio_relativo: Desvio em relação a média móvel
    - score_confianca_mercado: Score final normalizado (0-100)
    """
    odate = parameters.get("odate")
    process_full_data = parameters.get("process_full_data", False)
    logger.info("Parameters - Odate: %s, Full Data: %s", odate, process_full_data)

    df_ifix['dat_ref'] = pd.to_datetime(df_ifix['dat_ref'], errors='coerce').dt.strftime('%Y-%m-%d')
    df_ifix = df_ifix.sort_values('dat_ref', ascending=True)

    if not process_full_data:
        lookback_days = max(parms_indicador.get("rolling_return_window", 3), parms_indicador.get("lookback_days", 30))
        data_limite = (pd.to_datetime(odate) - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        logger.info("Data limite: %s (lookback_days=%d)", data_limite, lookback_days)

        df_ifix = df_ifix[(df_ifix['dat_ref'] >= data_limite) & (df_ifix['dat_ref'] <= odate)]

    df_ifix['retorno'] = df_ifix['close_price'].pct_change() * 100

    # calcular média móvel
    df_ifix['retorno_mm'] = df_ifix['retorno'].rolling(parms_indicador.get("rolling_return_window", 3)).mean()

    # desvio relativo (se retorno está acima ou abaixo da média recente)
    df_ifix['desvio_relativo'] = (df_ifix['retorno'] / df_ifix['retorno_mm']) - 1

    # normalizar
    df_ifix['score_confianca_mercado'] = normalizar_escala(df_ifix['desvio_relativo'])

    if not process_full_data:
        df_ifix = df_ifix[df_ifix["dat_ref"] == odate]

    return df_ifix[['dat_ref', 'retorno', 'retorno_mm', 'desvio_relativo', 'score_confianca_mercado']]


def indicador_sentimento_midia(df_rw_infomoney, df_rw_moneytimes, df_rw_seudinheiro, df_rw_valorinveste, parameters: dict):
    """
    Calcula indicador de sentimento da mídia.

    Combina notícias de múltiplas fontes de mídia financeira, aplica análise
    de sentimento nos títulos e gera um score diário.

    Score próximo de 0: Sentimento muito negativo
    Score próximo de 50: Sentimento neutro
    Score próximo de 100: Sentimento muito positivo

    - score_noticias: Score médio diário de sentimento (0-100)
    """
    odate = parameters.get("odate")
    process_full_data = parameters.get("process_full_data", False)
    logger.info("Parameters - Odate: %s, Full Data: %s", odate, process_full_data)

    df = pd.concat([df_rw_infomoney, df_rw_moneytimes, df_rw_seudinheiro, df_rw_valorinveste], ignore_index=True)
    df = df.drop_duplicates(subset=['fonte', 'titulo', 'link'])

    if not process_full_data:
        df = df[df["dat_ref"] == odate]

    sentimentos = df["titulo"].apply(analisar_sentimento)
    df_sent = pd.DataFrame(sentimentos.tolist())
    df = pd.concat([df, df_sent], axis=1)

    # score
    df["score_sentimento"] = (df["positivo"] - df["negativo"]) * 100 / (df[["positivo", "negativo"]].sum(axis=1) + 1e-5)

    # normalizar para 0–100 (percentis robustos)
    df['score_noticias'] = normalizar_escala(df['score_sentimento'])
    sentimento_dia = df.groupby(df['dat_ref'])['score_noticias'].mean().reset_index()

    return sentimento_dia
