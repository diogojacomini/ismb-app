"""
This is a boilerplate pipeline 'data_processing' generated using Kedro 0.19.14
"""
import pandas as pd
import numpy as np
from .utils import (
    ewma_volatility,
    normalizar_escala,
    analisar_sentimento,
    filtrar_noticias_financeiras,
    calcular_score_dia,
    logger
)


def indicador_risco_credito(df: pd.DataFrame, dim_tempo: pd.DataFrame, parms_indicador: dict, parameters: dict = None) -> pd.DataFrame:
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

    if parameters.get("environment") == 'test':
        return _make_data_test('indicador_risco_credito', odate)

    df = df[df['cod_indice'] == 'CDS']

    df['dat_ref'] = pd.to_datetime(df['dat_ref'], errors='coerce').dt.strftime('%Y-%m-%d')
    df = df.sort_values('dat_ref', ascending=True)
    if not process_full_data:
        lookback_days = max(int(parms_indicador.get("window", 30) * 1.4 * 6), 252)  # 252 = dias úteis em 1 ano
        data_limite = (pd.to_datetime(odate) - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        logger.info("Data limite: %s (lookback_days=%d)", data_limite, lookback_days)

        df = df[(df['dat_ref'] >= data_limite) & (df['dat_ref'] <= odate)]
    else:
        dim_tempo_d = dim_tempo[dim_tempo['dia_util'] == 1][['dat_ref', 'ano']]
        df = pd.merge(df, dim_tempo_d, on='dat_ref', how='inner')

    df = ewma_volatility(df, parms_indicador.get("variacia", 21), lambda_=parms_indicador.get("lambda_ewma", 0.94))
    df['retorno_diario'] = df['val_fechamento'].pct_change(periods=parms_indicador.get("window", 30))

    df = df.dropna(subset=['vol_ewma', 'retorno_diario'])
    df['rank_vol'] = df['vol_ewma'].rank(pct=True)
    df['rank_retorno'] = (-df['retorno_diario']).rank(pct=True)

    # indicador Composto
    df['risco_bruto'] = (parms_indicador.get("w_vol", 0.5) * df['rank_vol']) + (parms_indicador.get("w_retorno", 0.5) * df['rank_retorno'])

    # normalização 0-100 (0 = medo extremo, 100 = ganância extrema)
    df['score_risco_credito'] = (1 - df['risco_bruto']) * 100

    # cod_indicador
    df['cod_indicador'] = parms_indicador.get("cod_indicador")

    if not process_full_data:
        df = df[df["dat_ref"] == odate]
    else:
        df = df[df["ano"] >= 2018]

    return df[parms_indicador.get("schema")]


def indicador_retorno_mercado(df_ibov: pd.DataFrame, dim_tempo: pd.DataFrame, parms_indicador: dict, parameters: dict) -> pd.DataFrame:
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

    if parameters.get("environment") == 'test':
        return _make_data_test('indicador_retorno_mercado', odate)

    df_ibov = df_ibov[df_ibov['cod_indice'] == 'IBOV']

    df_ibov['dat_ref'] = pd.to_datetime(df_ibov['dat_ref'], errors='coerce').dt.strftime('%Y-%m-%d')
    df_ibov = df_ibov.sort_values('dat_ref', ascending=True)

    if not process_full_data:
        lookback_days = max(65, parms_indicador.get("lookback_days", 180))  # 65 dias para médias + buffer para normalização
        data_limite = (pd.to_datetime(odate) - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        logger.info("Data limite: %s (lookback_days=%d)", data_limite, lookback_days)

        df_ibov = df_ibov[(df_ibov['dat_ref'] >= data_limite) & (df_ibov['dat_ref'] <= odate)]
    else:
        dim_tempo_d = dim_tempo[dim_tempo['dia_util'] == 1][['dat_ref', 'ano']]
        df_ibov = pd.merge(df_ibov, dim_tempo_d, on='dat_ref', how='inner')

    #  retorno logarítmico
    df_ibov['log_ret'] = np.log(df_ibov['val_fechamento'] / df_ibov['val_fechamento'].shift(1))

    # média e desvio padrão móvel (21 dias úteis)
    df_ibov['media_ret'] = df_ibov['log_ret'].rolling(window=parms_indicador.get('rolling_mean_window', 21)).mean()
    df_ibov['desvio_ret'] = df_ibov['log_ret'].rolling(window=parms_indicador.get('rolling_std_window', 21)).std()

    # Z-score do retorno
    df_ibov['z_retorno'] = (df_ibov['log_ret'] - df_ibov['media_ret']) / df_ibov['desvio_ret']

    # normalizar z-score (0–100)
    df_ibov['score'] = normalizar_escala(df_ibov['z_retorno'])

    # ponderar por volume (volume do dia / média 30 dias)
    df_ibov['media_vol'] = df_ibov['qtd_volume'].rolling(parms_indicador.get('volume_mean_window', 30)).mean()
    fator_vol = (df_ibov['qtd_volume'] / df_ibov['media_vol']).clip(lower=parms_indicador.get('volume_clip', 0.7))
    df_ibov['score_ponderado'] = df_ibov['score'] * fator_vol

    df_ibov['score_retorno_mercado'] = normalizar_escala(df_ibov['score_ponderado'])

    # cod_indicador
    df_ibov['cod_indicador'] = parms_indicador.get("cod_indicador")

    if not process_full_data:
        df_ibov = df_ibov[df_ibov["dat_ref"] == odate]
    else:
        df_ibov = df_ibov[df_ibov["ano"] >= 2018]

    return df_ibov[parms_indicador.get("schema")]


def indicador_volatilidade_mercado(df_consolidado: pd.DataFrame, dim_tempo: pd.DataFrame, parms_indicador: dict, parameters: dict) -> pd.DataFrame:
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
    
    if parameters.get("environment") == 'test':
        return _make_data_test('indicador_volatilidade_mercado', odate)

    df_consolidado['dat_ref'] = pd.to_datetime(df_consolidado['dat_ref'], errors='coerce').dt.strftime('%Y-%m-%d')
    df_ibov = df_consolidado[df_consolidado['cod_indice'] == 'IBOV']
    df_ivvb = df_consolidado[df_consolidado['cod_indice'] == 'IVVB']

    df_ibov = df_ibov.sort_values('dat_ref', ascending=True)
    df_ivvb = df_ivvb.sort_values('dat_ref', ascending=True)

    if not process_full_data:
        lookback_days = max(65, parms_indicador.get("lookback_days", 180))  # 65 dias para cálculos + buffer para normalização
        data_limite = (pd.to_datetime(odate) - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        logger.info("Data limite: %s (lookback_days=%d)", data_limite, lookback_days)

        df_ibov = df_ibov[(df_ibov['dat_ref'] >= data_limite) & (df_ibov['dat_ref'] <= odate)]
        df_ivvb = df_ivvb[(df_ivvb['dat_ref'] >= data_limite) & (df_ivvb['dat_ref'] <= odate)]

    else:
        dim_tempo_d = dim_tempo[dim_tempo['dia_util'] == 1][['dat_ref', 'ano']]
        df_ibov = pd.merge(df_ibov, dim_tempo_d, on='dat_ref', how='inner')
        df_ivvb = pd.merge(df_ivvb, dim_tempo_d, on='dat_ref', how='inner')

    #  retorno logarítmico
    df_ibov['log_ret'] = np.log(df_ibov['val_fechamento'] / df_ibov['val_fechamento'].shift(1))

    # EWMA (volatilidade histórica com alpha ~0.06)
    df_ibov['ewma_var'] = df_ibov['log_ret'].ewm(alpha=1 - parms_indicador.get('alpha_ewma', 0.94)).var()
    df_ibov['ewma_vol'] = np.sqrt(df_ibov['ewma_var']) * np.sqrt(252) * 100

    # ATR (volatilidade intradiária)
    high_low = df_ibov['val_maxima'] - df_ibov['val_minima']
    high_close = np.abs(df_ibov['val_maxima'] - df_ibov['val_fechamento'].shift(1))
    low_close = np.abs(df_ibov['val_minima'] - df_ibov['val_fechamento'].shift(1))
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df_ibov['atr'] = tr.rolling(window=parms_indicador.get('atr_window', 14)).mean()

    # IVVB11 (proxy de percepção global / VIX Brasil)
    df_ivvb['retorno_ivvb'] = df_ivvb['val_fechamento'].pct_change()

    # Volatilidade histórica do IVVB11 (EWMA de retornos)
    df_ivvb['vol_ivvb'] = df_ivvb['retorno_ivvb'].ewm(alpha=parms_indicador.get('alpha_ivvb', 0.06)).std() * np.sqrt(252) * 100

    # Normalização dos scores
    df_ibov['score_ewma'] = normalizar_escala(df_ibov['ewma_vol'])
    df_ibov['score_atr'] = normalizar_escala(df_ibov['atr'])
    df_ivvb['score_ivvb'] = normalizar_escala(df_ivvb['vol_ivvb'])
    
    columns_ibov = ['dat_ref', 'log_ret', 'ewma_var', 'ewma_vol', 'atr', 'score_ewma', 'score_atr']
    columns_ivvb = ['dat_ref', 'retorno_ivvb', 'vol_ivvb', 'score_ivvb']
    if process_full_data:
        columns_ivvb.append('ano')

    df_score = pd.merge(df_ibov[columns_ibov], df_ivvb[columns_ivvb], on='dat_ref', how='inner')
    df_score = df_score.dropna()

    # Score final ponderado
    df_score['score_volatilidade_mercado'] = 100 - (
        df_score['score_ewma'] * parms_indicador.get('ewma_weight', 0.5) +
        df_score['score_atr'] * parms_indicador.get('atr_weight', 0.3) +
        df_score['score_ivvb'] * parms_indicador.get('ivvb_weight', 0.2)
    )

    # cod_indicador
    df_score['cod_indicador'] = parms_indicador.get("cod_indicador")

    if not process_full_data:
        df_score = df_score[df_score["dat_ref"] == odate]
    else:
        df_score = df_score[df_score["ano"] >= 2018]

    return df_score[parms_indicador.get("schema")]


def indicador_atividade_mercado(df_ibov: pd.DataFrame, dim_tempo: pd.DataFrame, parms_indicador: dict, parameters: dict) -> pd.DataFrame:
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

    if parameters.get("environment") == 'test':
        return _make_data_test('indicador_atividade_mercado', odate)

    df_ibov = df_ibov[df_ibov['cod_indice'] == 'IBOV']

    df_ibov['dat_ref'] = pd.to_datetime(df_ibov['dat_ref'], errors='coerce').dt.strftime('%Y-%m-%d')
    df_ibov = df_ibov.sort_values('dat_ref', ascending=True)

    if not process_full_data:
        lookback_days = max(60, parms_indicador.get("lookback_days", 180))
        data_limite = (pd.to_datetime(odate) - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        logger.info("Data limite: %s (lookback_days=%d)", data_limite, lookback_days)

        df_ibov = df_ibov[(df_ibov['dat_ref'] >= data_limite) & (df_ibov['dat_ref'] <= odate)]

    else:
        dim_tempo_d = dim_tempo[dim_tempo['dia_util'] == 1][['dat_ref', 'ano']]
        df_ibov = pd.merge(df_ibov, dim_tempo_d, on='dat_ref', how='inner')

    # retorno diário
    df_ibov['retorno'] = df_ibov['val_fechamento'].pct_change() * 100

    # desvio em relação a média móvel (3 dias)
    mm3 = df_ibov['retorno'].rolling(parms_indicador.get('rolling_return_window', 3)).mean()
    df_ibov['desvio_relativo'] = (df_ibov['retorno'] / mm3) - 1

    # normalizar o score de desvio entre 0–100 com base nos percentis históricos
    df_ibov['score'] = normalizar_escala(df_ibov['desvio_relativo'])

    # ponderar score com volume (volume do dia / média dos últimos 30 dias)
    media_vol = df_ibov['qtd_volume'].rolling(parms_indicador.get('volume_mean_window', 30)).mean()
    fator_vol = (df_ibov['qtd_volume'] / media_vol).clip(lower=parms_indicador.get('volume_clip', 0.7))
    df_ibov['score_atividade_mercado'] = df_ibov['score'] * fator_vol
    df_ibov['score_atividade_mercado'] = normalizar_escala(df_ibov['score_atividade_mercado'])

    # cod_indicador
    df_ibov['cod_indicador'] = parms_indicador.get("cod_indicador")

    if not process_full_data:
        df_ibov = df_ibov[df_ibov["dat_ref"] == odate]
    else:
        df_ibov = df_ibov[df_ibov["ano"] >= 2018]

    return df_ibov[parms_indicador.get("schema")]


def indicador_confianca_mercado_local(df_ifix: pd.DataFrame, dim_tempo: pd.DataFrame, parms_indicador: dict, parameters: dict) -> pd.DataFrame:
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

    if parameters.get("environment") == 'test':
        return _make_data_test('indicador_confianca_mercado_local', odate)

    df_ifix = df_ifix[df_ifix['cod_indice'] == 'IFIX']

    df_ifix['dat_ref'] = pd.to_datetime(df_ifix['dat_ref'], errors='coerce').dt.strftime('%Y-%m-%d')
    df_ifix = df_ifix.sort_values('dat_ref', ascending=True)

    if not process_full_data:
        lookback_days = max(parms_indicador.get("rolling_return_window", 3), parms_indicador.get("lookback_days", 30))
        data_limite = (pd.to_datetime(odate) - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        logger.info("Data limite: %s (lookback_days=%d)", data_limite, lookback_days)

        df_ifix = df_ifix[(df_ifix['dat_ref'] >= data_limite) & (df_ifix['dat_ref'] <= odate)]
    else:
        dim_tempo_d = dim_tempo[dim_tempo['dia_util'] == 1][['dat_ref', 'ano']]
        df_ifix = pd.merge(df_ifix, dim_tempo_d, on='dat_ref', how='inner')

    df_ifix['retorno'] = df_ifix['val_fechamento'].pct_change() * 100

    # calcular média móvel
    df_ifix['retorno_mm'] = df_ifix['retorno'].rolling(parms_indicador.get("rolling_return_window", 3)).mean()

    # desvio relativo (se retorno está acima ou abaixo da média recente)
    df_ifix['desvio_relativo'] = (df_ifix['retorno'] / df_ifix['retorno_mm']) - 1

    # normalizar
    df_ifix['score_confianca_mercado'] = normalizar_escala(df_ifix['desvio_relativo'])

    # cod_indicador
    df_ifix['cod_indicador'] = parms_indicador.get("cod_indicador")

    if not process_full_data:
        df_ifix = df_ifix[df_ifix["dat_ref"] == odate]
    else:
        df_ifix = df_ifix[df_ifix["ano"] >= 2018]

    return df_ifix[parms_indicador.get("schema")]


def indicador_sentimento_midia(df_consolidated_noticias, parms_indicador, parameters: dict):
    """
    Calcula o indicador de sentimento da mídia financeira.

    Melhorias v2:
      1. Pré-filtro de relevância — mantém apenas notícias de economia,
         mercado e política; descarta off-topic (esportes, entretenimento…).
      2. Léxico VADER calibrado para PT-BR financeiro — termos como "dispara",
         "recessão", "calote", "dividendo" têm polaridade ajustada ao contexto.
      3. Score volátil via tanh amplificado — elimina a compressão ao centro
         típica de médias simples; sinais leves já afastam o score de 50.
      4. Ponderação por força do sinal — títulos com |compound| alto contam
         mais no score diário; títulos quase-neutros são descartados.

    Parâmetros relevantes (parameters_data_processing.yml):
      amplificacao       — fator k do tanh (default 2.0; maior = mais volátil)
      threshold_neutro   — títulos com neutro > valor são ignorados (default 0.80)
      min_compound_abs   — |compound| mínimo para considerar o título (default 0.05)
      min_noticias_dia   — mínimo de títulos válidos por dia (default 3)

    Score:
      0   → Sentimento muito negativo
      50  → Equilíbrio neutro
      100 → Sentimento muito positivo
    """
    odate             = parameters.get("odate")
    process_full_data = parameters.get("process_full_data", False)
    amplificacao      = parms_indicador.get("amplificacao",     2.0)
    threshold_neutro  = parms_indicador.get("threshold_neutro", 0.80)
    min_compound_abs  = parms_indicador.get("min_compound_abs", 0.05)
    min_noticias_dia  = parms_indicador.get("min_noticias_dia", 3)

    logger.info("Parameters - Odate: %s, Full Data: %s", odate, process_full_data)

    if parameters.get("environment") == 'test':
        return _make_data_test('indicador_sentimento_midia', odate)

    df = df_consolidated_noticias.copy()
    df['dat_ref'] = pd.to_datetime(df['dat_ref'], errors='coerce').dt.strftime('%Y-%m-%d')
    df = df.drop_duplicates(subset=['txt_titulo'])

    # ── Janela temporal ───────────────────────────────────────────────────────
    if not process_full_data:
        lookback_days = parms_indicador.get("lookback_days", 3)
        data_limite   = (pd.to_datetime(odate) - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        logger.info("Janela: %s → %s (lookback=%d)", data_limite, odate, lookback_days)
        df = df[(df['dat_ref'] >= data_limite) & (df['dat_ref'] <= odate)].copy()
        df['dat_ref'] = odate

    if df.empty:
        logger.warning("Nenhuma notícia no período — retornando DataFrame vazio.")
        return pd.DataFrame(columns=['dat_ref', 'cod_indicador', 'score_noticias'])

    # ── 1. Pré-filtro de relevância ───────────────────────────────────────────
    df['txt_titulo'] = df['txt_titulo'].fillna('').astype(str)
    df = filtrar_noticias_financeiras(df, coluna='txt_titulo')

    if df.empty:
        logger.warning("Nenhuma notícia relevante após filtro de relevância.")
        return pd.DataFrame(columns=['dat_ref', 'cod_indicador', 'score_noticias'])

    # ── 2. Análise de sentimento (VADER + léxico PT-BR) ───────────────────────
    sentimentos = df['txt_titulo'].apply(analisar_sentimento)
    df_sent     = pd.DataFrame(list(sentimentos))
    df = pd.concat([df.reset_index(drop=True), df_sent.reset_index(drop=True)], axis=1)

    # ── 3. Score diário volátil com ponderação por força do sinal ─────────────
    scores_por_dia = []

    for dat, grupo in df.groupby('dat_ref'):
        total_titulos = len(grupo)
        score = calcular_score_dia(
            grupo,
            amplificacao=amplificacao,
            threshold_neutro=threshold_neutro,
            min_compound_abs=min_compound_abs,
        )

        if score is None:
            # Menos títulos válidos que o mínimo: usa 50 (neutro) com aviso
            validos = int((grupo['compound'].abs() >= min_compound_abs).sum())
            logger.warning(
                "Data %s: apenas %d/%d títulos válidos (min=%d) → score=50 (neutro).",
                dat, validos, total_titulos, min_noticias_dia,
            )
            score = 50.0

        validos = int(
            ((grupo['compound'].abs() >= min_compound_abs) &
             (grupo['neutro'] <= threshold_neutro)).sum()
        )
        logger.info(
            "Data %s: %d válidos / %d totais → score=%.2f",
            dat, validos, total_titulos, score,
        )
        scores_por_dia.append({
            'dat_ref':       dat,
            'cod_indicador': parms_indicador.get("cod_indicador"),
            'score_noticias': round(score, 4),
        })

    sentimento_dia = pd.DataFrame(scores_por_dia)
    return sentimento_dia[parms_indicador.get("schema")]


def _make_data_test(indicador, odate):
    
    if indicador == 'indicador_risco_credito':
        return pd.DataFrame({"dat_ref": [odate],
                             "cod_indicador": ['RISCO_CREDITO'],
                             "retorno_diario": [999.99],
                             "vol_ewma": [9999],
                             "rank_vol": [9999],
                             "rank_retorno": [9999],
                             "risco_bruto": [9999],
                             "score_risco_credito":[59.99]
                            })

    elif indicador == 'indicador_retorno_mercado':
        return pd.DataFrame({"dat_ref": [odate],
                             "cod_indicador": ['RETORNO_MERCADO'],
                             "log_ret": [999.99],
                             "media_ret": [9999],
                             "desvio_ret": [9999],
                             "z_retorno": [9999],
                             "media_vol": [9999],
                             "score_retorno_mercado":[59.99]
                            })

    elif indicador == 'indicador_volatilidade_mercado':
        return pd.DataFrame({"dat_ref": [odate],
                             "cod_indicador": ['VOLATILIDADE'],
                             "log_ret": [999.99],
                             "ewma_var": [9999],
                             "ewma_vol": [9999],
                             "atr": [9999],
                             "score_ewma": [9999],
                             "score_atr": [9999],
                             "retorno_ivvb": [9999],
                             "vol_ivvb": [9999],
                             "score_ivvb": [9999],
                             "score_volatilidade_mercado":[59.99]
                         })

    elif indicador == 'indicador_atividade_mercado':
        return pd.DataFrame({"dat_ref": [odate],
                             "cod_indicador": ['ATIVIDADE'],
                             "retorno": [999.99],
                             "desvio_relativo": [9999],
                             "score": [9999],
                             "score_atividade_mercado":[59.99]
                         })

    elif indicador == 'indicador_confianca_mercado_local':
        return pd.DataFrame({"dat_ref": [odate],
                             "cod_indicador": ['CONFIANCA_LOCAL'],
                             "retorno": [999.99],
                             "retorno_mm": [9999],
                             "desvio_relativo": [9999],
                             "score_confianca_mercado":[59.99]
                         })

    elif indicador == 'indicador_sentimento_midia':
        return pd.DataFrame({"dat_ref": [odate],
                             "cod_indicador": ['SENTIMENTO_NOTICIAS'],
                             "score_noticias":[59.99]
                         })
    else:
        return pd.DataFrame()
