from pathlib import Path


class DataPaths:
    """Centralised path constants for every data file the API reads."""

    REPO_ROOT = Path(__file__).resolve().parents[4]
    FACTORY_DIR = REPO_ROOT / "factory"

    # ── curated layer ─────────────────────────────────────────────────────
    FACTS_DIR = FACTORY_DIR / "data" / "02_curated" / "facts"
    INDICE_PATH = FACTS_DIR / "fato_indice_ismb.csv"
    MERCADO_PATH = FACTS_DIR / "fato_transacao_mercado.csv"

    # ── analytics layer ───────────────────────────────────────────────────
    ANALYTICS_DIR = FACTORY_DIR / "data" / "04_analytics"
    SERIE_TEMPORAL_PATH = ANALYTICS_DIR / "analytics_serie_temporal_ismb.csv"
    CORRELACAO_PATH = ANALYTICS_DIR / "analytics_correlacao.csv"
    KPIS_PATH = ANALYTICS_DIR / "analytics_kpis_agregados.csv"
    DASHBOARD_DIARIO_PATH = ANALYTICS_DIR / "analytics_dashboard_diario.csv"

    # ── indicators layer ──────────────────────────────────────────────────
    INDICATORS_DIR = FACTORY_DIR / "data" / "03_indicators"
    INDICATOR_FILES: dict[str, str] = {
        "risco_credito": "indicador_risco_credito.csv",
        "retorno_mercado": "indicador_retorno_mercado.csv",
        "volatilidade_mercado": "indicador_volatilidade_mercado.csv",
        "atividade_mercado": "indicador_atividade_mercado.csv",
        "confianca_mercado_local": "indicador_confianca_mercado_local.csv",
        "sentimento_noticias": "indicador_sentimento_noticias.csv",
    }

    # ── governance layer ──────────────────────────────────────────────────
    GOVERNANCE_DIR = FACTORY_DIR / "data" / "00_governance" / "data_quality"
    QUALITY_PATH = GOVERNANCE_DIR / "data_quality_report.csv"
