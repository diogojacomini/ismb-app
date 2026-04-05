import re

import numpy as np
import pandas as pd
import logging
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

logger = logging.getLogger(__name__)

try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon')

# =============================================================================
# Léxico financeiro PT-BR para VADER
# Escala: −4 (muito negativo) -> +4 (muito positivo), 0 = neutro
# Calibrado para títulos de portais financeiros brasileiros.
# =============================================================================
FINANCIAL_LEXICON: dict[str, float] = {
    # ── Mercado / índices ─────────────────────────────────────────────────────
    "alta":            2.5,  "altas":           2.3,
    "valoriza":        2.4,  "valorização":     2.4,  "valorizado":      2.0,
    "sobe":            2.0,  "subida":          2.0,  "sobe forte":      3.0,
    "dispara":         3.0,  "disparo":         3.0,  "disparam":        3.0,
    "avança":          2.0,  "avançam":         2.0,  "avança forte":    2.8,
    "rali":            3.0,  "rally":           3.0,
    "recorde":         2.5,  "máxima":          2.5,  "máximas":         2.3,
    "recuperação":     2.0,  "recupera":        2.0,  "recuperam":       2.0,
    "retomada":        2.0,  "retoma":          2.0,
    "aceleração":      1.8,  "acelera":         1.8,
    "superávit":       2.5,  "crescimento":     2.0,  "cresce":          1.8,
    "expansão":        2.0,  "expande":         1.8,
    "lucro":           2.5,  "lucros":          2.5,  "lucrativo":       2.3,
    "dividendo":       2.0,  "dividendos":      2.0,  "proventos":       1.8,
    "compra":          1.5,  "compras":         1.5,  "compradores":     1.5,
    "demanda":         1.5,  "aquecimento":     1.8,
    "otimismo":        2.5,  "otimista":        2.3,  "otimistas":       2.3,
    "confiança":       2.0,  "expectativa positiva": 2.5,
    "estabilidade":    1.5,  "estável":         1.0,
    "aprovação":       1.8,  "aprovado":        1.8,  "aprovada":        1.8,
    "acordo":          1.8,  "acordos":         1.5,  "parceria":        1.5,
    "investimento":    1.8,  "investimentos":   1.8,  "capta":           1.5,
    "captação":        1.5,  "ipo":             1.5,  "listagem":        1.5,
    "exportação":      1.5,  "exportações":     1.5,  "exporta":         1.5,
    "emprego":         1.8,  "empregos":        1.8,  "contratação":     1.8,
    "resultados positivos": 2.5,
    "revisão positiva": 2.5, "upgrade":         2.0,  "recomendação de compra": 3.0,
    "superou expectativas": 3.0, "bateu expectativas": 3.0,
    "forte desempenho": 2.5, "bom resultado":   2.5,
    "ganho":           2.0,  "ganhos":          2.0,
    "positivo":        1.8,  "positiva":        1.8,  "positivas":       1.8,
    "juros em queda":  2.5,  "corte de juros":  2.5,  "queda nos juros": 2.5,
    "inflação cede":   2.5,  "deflação":        1.5,
    "bolsa sobe":      3.0,  "ibovespa sobe":   3.0,  "mercado sobe":    3.0,
    "dólar cai":       2.0,  "câmbio estável":  1.5,

    # ── Queda / crise ─────────────────────────────────────────────────────────
    "queda":          -2.5,  "quedas":          -2.3,
    "cai":            -2.0,  "caem":            -2.0,  "caiu":           -2.0,
    "desaba":         -3.0,  "desabam":         -3.0,  "desacelera":     -2.0,
    "afunda":         -3.0,  "afundam":         -3.0,
    "despenca":       -3.0,  "despencam":       -3.0,
    "recua":          -1.8,  "recuam":          -1.8,  "recuo":          -1.8,
    "perde":          -2.0,  "perdem":          -2.0,  "perdas":         -2.5,
    "baixa":          -2.0,  "baixas":          -2.0,  "mínima":         -2.0,  "mínimas": -2.0,
    "recessão":       -3.5,  "recessão técnica": -4.0,
    "crise":          -3.0,  "crises":          -3.0,
    "colapso":        -4.0,  "colapsos":        -4.0,
    "falência":       -4.0,  "faliu":           -4.0,  "insolvência":    -4.0,
    "inadimplência":  -3.0,  "inadimplente":    -3.0,
    "calote":         -3.5,  "default":         -3.5,
    "dívida":         -1.5,  "endividamento":   -2.0,  "endividado":     -2.0,
    "deficit":        -2.5,  "déficit":         -2.5,
    "inflação sobe":  -2.5,  "inflação acelera": -2.5,  "inflação alta":  -3.0,
    "juros sobem":    -2.5,  "alta de juros":   -2.5,  "alta nos juros": -2.5,
    "aperto monetário": -2.5,
    "pessimismo":     -2.5,  "pessimista":      -2.5,  "incerteza":      -2.0,
    "insegurança":    -2.0,  "instabilidade":   -2.5,
    "vende":          -1.2,  "vendem":          -1.2,  "vendedores":     -1.2,
    "fuga":           -3.0,  "fuga de capitais": -3.5,
    "desemprego":     -2.5,  "demissão":        -2.5,  "demissões":      -2.8,
    "layoff":         -3.0,  "corte de empregos": -2.8,
    "prejuízo":       -3.0,  "prejuízos":       -3.0,
    "rebaixamento":   -3.0,  "rebaixado":       -3.0,  "downgrade":      -3.0,
    "abaixo do esperado": -2.5, "frustrou expectativas": -2.5,
    "resultado negativo": -2.5, "fraco resultado": -2.5,
    "tensão":         -2.0,  "turbulência":     -2.5,  "turbulento":     -2.5,
    "bolsa cai":      -3.0,  "ibovespa cai":    -3.0,  "mercado cai":    -3.0,
    "dólar sobe":     -2.0,  "câmbio pressiona": -2.0,
    "risco fiscal":   -2.5,  "risco político":  -2.5,
    "rombo":          -3.0,  "rombos":          -3.0,
    "fraude":         -3.5,  "fraudes":         -3.5,  "esquema":        -2.5,
    "corrupção":      -3.0,  "investigação":    -2.0,
    "sanção":         -2.5,  "sanções":         -2.5,  "embargo":        -2.5,
    "guerra":         -3.0,  "conflito":        -2.5,  "crise geopolítica": -3.0,
    "recessão global": -4.0,

    # ── Neutros com peso leve (contexto financeiro) ───────────────────────────
    "ata":             0.0,  "reunião":         0.0,  "ipca":            0.0,
    "copom":           0.0,  "bacen":           0.0,  "banco central":   0.0,
    "selic":           0.0,  "câmbio":          0.0,  "dólar":           0.0,
    "fiscal":         -0.3,  "reforma":         0.5,  "privatização":    0.8,
    "leilão":          0.5,  "resultado":       0.3,
}

# =============================================================================
# Filtro de relevância - notícias financeiras / econômicas / políticas
# =============================================================================
# Palavras que INCLUEM a notícia (ao menos uma deve estar no título)
_INCLUDE_PATTERNS: list[str] = [
    # mercado / finanças
    r"\bbolsa\b", r"\bibovespa\b", r"\bbmf\b", r"\bação\b", r"\bações\b",
    r"\bativo\b", r"\bativos\b", r"\bmercado\b", r"\binvestimento\b",
    r"\bdividendo\b", r"\bipo\b", r"\bfundo(s)?\b", r"\betf\b",
    r"\bcdi\b", r"\bselic\b", r"\bjuros\b", r"\binflação\b", r"\bipca\b",
    r"\bigpm\b", r"\bicm\b", r"\bcâmbio\b", r"\bdólar\b", r"\beuro\b",
    r"\breal\b", r"\bcds\b", r"\bspread\b", r"\brisco\b", r"\bcredit\b",
    r"\bbanco\b", r"\bbancos\b", r"\bfinanceiro\b", r"\bfinanceira\b",
    r"\beconomia\b", r"\beconômico\b", r"\bpib\b", r"\bgdp\b",
    r"\bfed\b", r"\bfederal reserve\b", r"\bbce\b", r"\bimf\b", r"\bfmi\b",
    r"\bpetrobras\b", r"\bvale\b", r"\bitaú\b", r"\bbradesco\b",
    r"\bsantander\b", r"\bunibanco\b", r"\bxp\b", r"\bb3\b",
    r"\bexportação\b", r"\bimportação\b", r"\bcomércio\b", r"\bbalança\b",
    r"\btrade\b", r"\btarifa\b", r"\btarifas\b", r"\bsanção\b",
    r"\bdébito\b", r"\bdívida\b", r"\bcrédito\b", r"\bfinancia\b",
    r"\blucro\b", r"\bprejuízo\b", r"\bresultado\b", r"\bbalanço\b",
    r"\breceita\b", r"\bcusto\b", r"\bmargen\b", r"\bmargem\b",
    r"\bebitda\b", r"\blpa\b", r"\broe\b", r"\broa\b",
    r"\bcrise\b", r"\brecessão\b", r"\brotatividade\b",
    r"\bemprego\b", r"\bdesemprego\b", r"\bcaged\b", r"\bpnad\b",
    # política econômica
    r"\bgoverno\b", r"\bministério\b", r"\bministro\b", r"\bpresidente\b",
    r"\bcongresso\b", r"\bsenado\b", r"\bcâmara\b", r"\blula\b",
    r"\bbolsonaro\b", r"\bhadd\b", r"\bguedes\b", r"\bhaddad\b",
    r"\breforma\b", r"\bpec\b", r"\bproposta\b", r"\bvoto\b",
    r"\bimposto\b", r"\btributo\b", r"\btaxa\b", r"\bisenção\b",
    r"\bfazenda\b", r"\bplanejamento\b", r"\borcamento\b", r"\borçamento\b",
    r"\bprivatização\b", r"\bestatal\b", r"\bconcessão\b",
    # geopolítica / global
    r"\bguerra\b", r"\bconflito\b", r"\botan\b", r"\bua\b", r"\bchina\b",
    r"\busa\b", r"\beua\b", r"\btrump\b", r"\bbiden\b", r"\bamérica\b",
    r"\bglobal\b", r"\bmundial\b", r"\binternacional\b",
    r"\bpetróleo\b", r"\bpetrol\b", r"\bcommodit\b", r"\bsoja\b",
    r"\bminério\b", r"\baço\b", r"\btrigo\b", r"\bmilho\b",
]

# Palavras que EXCLUEM a notícia (se presente -> descartado por ser off-topic)
_EXCLUDE_PATTERNS: list[str] = [
    r"\bfutebol\b", r"\bfutebol\b", r"\belenco\b", r"\btime\b",
    r"\bcampeonato\b", r"\bcopado\b", r"\bliga\b", r"\bgol\b",
    r"\bjogador\b", r"\btecnico\b", r"\btécnico\b", r"\bestádio\b",
    r"\bnba\b", r"\bnfl\b", r"\bnhl\b",
    r"\bnovela\b", r"\bserie\b", r"\bfilme\b", r"\bcinema\b",
    r"\bcelebrid\b", r"\bfamoso\b", r"\bfamosa\b", r"\bartista\b",
    r"\bcantor\b", r"\bsinger\b", r"\bmusica\b", r"\bmúsica\b",
    r"\bcasamento\b", r"\bdivorci\b", r"\bnamorad\b",
    r"\bhoróscopo\b", r"\bsigno\b",
    r"\bculinária\b", r"\breceit(a)?\b", r"\bgastronomia\b",
    r"\bturismo\b", r"\bviagem\b", r"\bférias\b",
    r"\bsaúde\b", r"\bvacina\b", r"\bgripe\b", r"\bcovid\b",
    r"\besporte\b", r"\bolimpíada\b", r"\bpan\b",
    r"\bmoda\b", r"\bbel(e)za\b",
]

_include_re = re.compile("|".join(_INCLUDE_PATTERNS), re.IGNORECASE)
_exclude_re = re.compile("|".join(_EXCLUDE_PATTERNS), re.IGNORECASE)


def filtrar_noticias_financeiras(df: pd.DataFrame, coluna: str = "txt_titulo") -> pd.DataFrame:
    """
    Mantém apenas notícias de relevância financeira, econômica ou política.
    Remove títulos off-topic (esportes, entretenimento, saúde, etc.).
    Retorna DataFrame filtrado com coluna 'relevante' para auditoria.
    """
    titulo = df[coluna].fillna("").astype(str)
    incluir = titulo.str.contains(_include_re)
    excluir = titulo.str.contains(_exclude_re)
    df = df[incluir & ~excluir].copy()
    logger.info("Filtro relevância: %d notícias relevantes", len(df))
    return df


# =============================================================================
# Análise de sentimento com léxico financeiro PT-BR
# =============================================================================

def _get_sia() -> SentimentIntensityAnalyzer:
    """Retorna VADER com léxico financeiro PT-BR injetado (singleton por processo)."""
    sia = SentimentIntensityAnalyzer()
    sia.lexicon.update(FINANCIAL_LEXICON)
    return sia


def analisar_sentimento(texto: str) -> dict:
    """
    Analisa sentimento de um título financeiro via VADER + léxico PT-BR.
    Retorna negativo, neutro, positivo, compound.
    """
    sia = _get_sia()
    score = sia.polarity_scores(str(texto))
    return {
        "negativo": score["neg"],
        "neutro":   score["neu"],
        "positivo": score["pos"],
        "compound": score["compound"],
    }


def score_sentimento_volatil(compound: float, amplificacao: float = 2.0) -> float:
    """
    Mapeia compound VADER para score [0, 100] usando tanh amplificado.

    tanh(k · c) é mais volátil que mapeamento linear:
      - compound = ±0.1 (leve)  -> score ≈ 60 / 40
      - compound = ±0.3 (médio) -> score ≈ 76 / 24
      - compound = ±0.6 (forte) -> score ≈ 91 / 9

    Args:
        compound:     Score compound VADER ∈ [-1, 1]
        amplificacao: Fator de amplificação k (padrão 2.0)
    """
    import math
    return 50.0 * (1.0 + math.tanh(amplificacao * compound))


def calcular_score_dia(
    df: pd.DataFrame,
    amplificacao: float = 2.0,
    threshold_neutro: float = 0.80,
    min_compound_abs: float = 0.05,
) -> float | None:
    """
    Calcula o score diário de sentimento a partir de um DataFrame de títulos já analisados.

    Estratégia de ponderação por força do sinal:
      - Títulos com |compound| < min_compound_abs são descartados (ruído puro)
      - Títulos com neutro > threshold_neutro são descartados
      - Cada título recebe peso = |compound| (títulos mais polares têm mais voz)
      - Score final = média ponderada dos scores individuais

    Returns None se não houver títulos válidos suficientes.
    """
    mask = (
        (df["compound"].abs() >= min_compound_abs) &
        (df["neutro"] <= threshold_neutro)
    )
    df_val = df[mask]

    if df_val.empty:
        return None

    scores = df_val["compound"].apply(lambda c: score_sentimento_volatil(c, amplificacao))
    pesos  = df_val["compound"].abs()

    return float((scores * pesos).sum() / pesos.sum())


# =============================================================================
# Utilitários de séries temporais
# =============================================================================

def ewma_volatility(df, variacia=21, lambda_=0.94):

    if df['val_fechamento'].isnull().any():
        raise ValueError("A coluna 'close' contém valores nulos.")

    df['retorno_diario'] = df['val_fechamento'].pct_change()
    var = df['retorno_diario'].dropna().var()

    variance = []
    for r in df['retorno_diario']:
        if pd.isna(r):
            variance.append(np.nan)
        else:
            var = lambda_ * var + (1 - lambda_) * (r ** 2)
            variance.append(var)

    df['vol_ewma'] = np.sqrt(variance) * np.sqrt(variacia)
    return df


def normalizar_escala(s):
    """Normaliza uma série para a escala 0-100 usando quantis 5% e 95%."""
    quantis = s.quantile([0.05, 0.95])
    p5  = quantis.loc[0.05]
    p95 = quantis.loc[0.95]

    def norm(x):
        if pd.isna(x):
            return np.nan
        if x <= p5:
            return 0
        if x >= p95:
            return 100
        return ((x - p5) / (p95 - p5)) * 100

    return s.apply(norm)

