import pandas as pd
import numpy as np
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification


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
    model_name = "nlptown/bert-base-multilingual-uncased-sentiment"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    inputs = tokenizer(texto, return_tensors="pt", truncation=True, padding=True)
    with torch.no_grad():
        logits = model(**inputs).logits
    probs = torch.nn.functional.softmax(logits, dim=1)[0]
    return {
        "negativo": probs[0].item(),
        "neutro": probs[1].item(),
        "positivo": probs[2].item()
    }
