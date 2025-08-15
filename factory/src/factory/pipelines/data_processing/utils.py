import pandas as pd
import numpy as np
import logging
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

logger = logging.getLogger(__name__)

try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon')


def ewma_volatility(df, variacia=21, lambda_=0.94):

    if df['close_price'].isnull().any():
        raise ValueError("A coluna 'close' contém valores nulos.")

    df['retorno_diario'] = df['close_price'].pct_change()
    var = df['retorno_diario'].dropna().var()  # variância

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
    """Normaliza uma série para a escala 0-100, considerando os quantis 5% e 95% como limites."""
    quantis = s.quantile([0.05, 0.95])
    p5 = quantis.loc[0.05]
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


def analisar_sentimento(texto):
    sia = SentimentIntensityAnalyzer()
    score = sia.polarity_scores(texto)
    return {
        "negativo": score["neg"],
        "neutro": score["neu"],
        "positivo": score["pos"],
        "compound": score["compound"]
    }
