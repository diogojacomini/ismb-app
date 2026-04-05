"""
PostgreSQL Data Access Layer.

Camada de acesso a dados que lê diretamente do PostgreSQL.
Todas as funções públicas são decoradas com @cached() para performance consistente.

Principais componentes:
    - TableNames: Mapeamento centralizado de nomes de tabelas
    - Funções de leitura cacheadas (read_indice, read_mercado, etc.)
    - Helpers de conversão de tipos (PostgreSQL -> JSON)

Padrões:
    - TTL de cache: 60 segundos
    - Tipos retornados: List[Dict[str, Any]]
    - Valores numéricos: float (nunca Decimal ou int)
    - Datas: string ISO (YYYY-MM-DD)
    - NaN/Infinity: None (JSON compliant)
"""

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List
from ..core.cache import cached
from ..core.database import execute_query


class TableNames:
    """Central registry of PostgreSQL table names."""

    # Curated layer
    FATO_INDICE_ISMB = "curated.fato_indice_ismb"
    FATO_TRANSACAO_MERCADO = "curated.fato_transacao_mercado"

    # Analytics layer
    ANALYTICS_SERIE_TEMPORAL = "analytics.analytics_serie_temporal_ismb"
    ANALYTICS_CORRELACAO = "analytics.analytics_correlacao"
    ANALYTICS_KPIS = "analytics.analytics_kpis_agregados"
    ANALYTICS_DASHBOARD = "analytics.analytics_dashboard_diario"

    # Indicators layer
    INDICADOR_RISCO_CREDITO = "indicators.indicador_risco_credito"
    INDICADOR_RETORNO_MERCADO = "indicators.indicador_retorno_mercado"
    INDICADOR_VOLATILIDADE_MERCADO = "indicators.indicador_volatilidade_mercado"
    INDICADOR_ATIVIDADE_MERCADO = "indicators.indicador_atividade_mercado"
    INDICADOR_CONFIANCA_MERCADO = "indicators.indicador_confianca_mercado_local"
    INDICADOR_SENTIMENTO_NOTICIAS = "indicators.indicador_sentimento_noticias"

    # Governance layer
    DATA_QUALITY_REPORT = "governance.data_quality_report"


def _coerce_numeric_values(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Coerce values to JSON-serializable types for consistency with CSV behavior.

    This ensures the API returns the same data types regardless of backend.
    - Decimal -> float (PostgreSQL numeric types)
    - date/datetime -> ISO string (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
    - NaN/Infinity -> None (not JSON compliant)
    - int -> float (for consistency)
    - None -> None
    - str -> str
    """
    result = []
    for row in rows:
        coerced = {}
        for k, v in row.items():
            if v is None:
                coerced[k] = None
            elif isinstance(v, Decimal):
                # Convert Decimal to float for JSON serialization
                float_val = float(v)
                # Check for NaN or Infinity (not JSON compliant)
                if math.isnan(float_val) or math.isinf(float_val):
                    coerced[k] = None
                else:
                    coerced[k] = float_val
            elif isinstance(v, datetime):
                # Convert datetime to ISO string (with time)
                coerced[k] = v.isoformat()
            elif isinstance(v, date):
                # Convert date to ISO string (YYYY-MM-DD)
                coerced[k] = v.isoformat()
            elif isinstance(v, (int, float)):
                # Check for NaN or Infinity in float values
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    coerced[k] = None
                else:
                    # Keep as numeric (convert int to float for consistency)
                    coerced[k] = float(v) if isinstance(v, int) else v
            else:
                # Keep strings and other types as-is
                coerced[k] = v
        result.append(coerced)
    return result


@cached(ttl=60)
def read_indice() -> List[Dict[str, Any]]:
    """
    Read ISMB index time series from curated.fato_indice_ismb.

    Returns:
        List of dicts with keys: dat_ref, indice_ismb, score_*, etc.
    """
    query = f"""
        SELECT
            dat_ref,
            indice_ismb,
            score_risco_credito,
            score_retorno_mercado,
            score_volatilidade_mercado,
            score_atividade_mercado,
            score_confianca_mercado,
            score_noticias
        FROM {TableNames.FATO_INDICE_ISMB}
        ORDER BY dat_ref ASC
    """
    rows = execute_query(query)
    return _coerce_numeric_values(rows)


@cached(ttl=60)
def read_serie_temporal() -> List[Dict[str, Any]]:
    """
    Read ISMB time series with technical indicators.

    Returns:
        List of dicts with keys: dat_ref, value, daily_return, ma_*, bb_*, etc.
    """
    query = f"""
        SELECT *
        FROM {TableNames.ANALYTICS_SERIE_TEMPORAL}
        ORDER BY dat_ref ASC
    """
    rows = execute_query(query)
    return _coerce_numeric_values(rows)


@cached(ttl=60)
def read_correlacao() -> List[Dict[str, Any]]:
    """
    Read correlation analysis between indices.

    Returns:
        List of dicts with keys: idx_a, idx_b, corr, n_obs, start_date, end_date, dat_ref
    """
    query = f"""
        SELECT
            idx_a,
            idx_b,
            corr,
            n_obs,
            start_date,
            end_date,
            dat_ref
        FROM {TableNames.ANALYTICS_CORRELACAO}
        ORDER BY corr DESC NULLS LAST
    """
    rows = execute_query(query)
    return _coerce_numeric_values(rows)


@cached(ttl=60)
def read_kpis() -> List[Dict[str, Any]]:
    """
    Read aggregated KPIs by entity and year.

    Returns:
        List of dicts with keys: entity_type, entity_name, year, annual_return,
        annual_volatility, max_drawdown, n_obs, dat_ref
    """
    query = f"""
        SELECT
            entity_type,
            entity_name,
            year,
            annual_return,
            annual_volatility,
            max_drawdown,
            n_obs,
            dat_ref
        FROM {TableNames.ANALYTICS_KPIS}
        ORDER BY entity_type, entity_name, year
    """
    rows = execute_query(query)
    return _coerce_numeric_values(rows)


@cached(ttl=60)
def read_dashboard_diario() -> List[Dict[str, Any]]:
    """
    Read daily dashboard data per index.

    Returns:
        List of dicts with keys: cod_indice, dat_ref, close, prev_close,
        abs_change, pct_change, volume, ismb_value, ismb_abs_change, ismb_pct_change
    """
    query = f"""
        SELECT
            cod_indice,
            dat_ref,
            close,
            prev_close,
            abs_change,
            pct_change,
            volume,
            ismb_value,
            ismb_abs_change,
            ismb_pct_change
        FROM {TableNames.ANALYTICS_DASHBOARD}
        ORDER BY dat_ref DESC, cod_indice ASC
    """
    rows = execute_query(query)
    return _coerce_numeric_values(rows)


@cached(ttl=60)
def read_quality() -> List[Dict[str, Any]]:
    """
    Read data quality report from governance layer.

    Returns:
        List of dicts with quality metrics, or empty list if table doesn't exist.
    """
    try:
        query = f"""
            SELECT *
            FROM {TableNames.DATA_QUALITY_REPORT}
            ORDER BY dat_ref DESC
        """
        rows = execute_query(query)
        return _coerce_numeric_values(rows)
    except Exception:
        # Table may not exist in all environments
        return []


@cached(ttl=60)
def read_indicador_full(nome: str) -> List[Dict[str, Any]]:
    """
    Read a specific indicator's full time series.

    Args:
        nome: Indicator name (risco_credito, retorno_mercado, etc.)

    Returns:
        List of dicts with indicator data

    Raises:
        FileNotFoundError: If indicator name is not recognized
    """
    # Map indicator names to tables
    indicator_tables = {
        "risco_credito": TableNames.INDICADOR_RISCO_CREDITO,
        "retorno_mercado": TableNames.INDICADOR_RETORNO_MERCADO,
        "volatilidade_mercado": TableNames.INDICADOR_VOLATILIDADE_MERCADO,
        "atividade_mercado": TableNames.INDICADOR_ATIVIDADE_MERCADO,
        "confianca_mercado_local": TableNames.INDICADOR_CONFIANCA_MERCADO,
        "sentimento_noticias": TableNames.INDICADOR_SENTIMENTO_NOTICIAS,
    }

    table = indicator_tables.get(nome.lower())
    if not table:
        raise FileNotFoundError(f"Indicator '{nome}' not found")

    query = f"""
        SELECT *
        FROM {table}
        ORDER BY dat_ref ASC
    """
    rows = execute_query(query)
    return _coerce_numeric_values(rows)


@cached(ttl=60)
def read_mercado() -> List[Dict[str, Any]]:
    """
    Read market transaction facts.

    Returns:
        List of dicts with keys: dat_ref, cod_indice, val_fechamento, etc.
    """
    query = f"""
        SELECT
            dat_ref,
            cod_indice,
            val_fechamento,
            val_abertura,
            val_minima,
            val_maxima,
            qtd_volume
        FROM {TableNames.FATO_TRANSACAO_MERCADO}
        ORDER BY dat_ref ASC, cod_indice ASC
    """
    rows = execute_query(query)
    return _coerce_numeric_values(rows)


# ── Analytics helpers (no caching needed - direct DB queries are fast) ────


def list_analytics() -> List[str]:
    """
    List available analytics tables.

    Returns:
        List of table names (simplified, without schema prefix)
    """
    return [
        "analytics_serie_temporal_ismb",
        "analytics_correlacao",
        "analytics_kpis_agregados",
        "analytics_dashboard_diario",
    ]


def read_analytics(filename: str) -> List[Dict[str, Any]]:
    """
    Read an analytics table by name (legacy CSV filename compatibility).

    Args:
        filename: Table name (with or without .csv extension)

    Returns:
        List of dicts from the specified table

    Raises:
        FileNotFoundError: If table name doesn't match any analytics table
    """
    # Remove .csv extension if present
    table_name = filename.replace(".csv", "").lower()

    # Map to full table name
    table_map = {
        "analytics_serie_temporal_ismb": TableNames.ANALYTICS_SERIE_TEMPORAL,
        "analytics_correlacao": TableNames.ANALYTICS_CORRELACAO,
        "analytics_kpis_agregados": TableNames.ANALYTICS_KPIS,
        "analytics_dashboard_diario": TableNames.ANALYTICS_DASHBOARD,
    }

    full_table = table_map.get(table_name)
    if not full_table:
        raise FileNotFoundError(f"Analytics table '{filename}' not found")

    query = f"SELECT * FROM {full_table} ORDER BY dat_ref DESC"
    rows = execute_query(query)
    return _coerce_numeric_values(rows)


def get_indicador(nome: str) -> List[Dict[str, Any]]:
    """
    Legacy fuzzy search for indicators (for backward compatibility).

    Args:
        nome: Partial indicator name to search

    Returns:
        List of dicts from matching indicator table

    Raises:
        FileNotFoundError: If no matching indicator found
    """
    nome_lower = nome.lower()

    # Try exact match first
    try:
        return read_indicador_full(nome_lower)
    except FileNotFoundError:
        pass

    # Try fuzzy match
    fuzzy_map = {
        "risco": "risco_credito",
        "credito": "risco_credito",
        "retorno": "retorno_mercado",
        "volatilidade": "volatilidade_mercado",
        "atividade": "atividade_mercado",
        "confianca": "confianca_mercado_local",
        "sentimento": "sentimento_noticias",
        "noticias": "sentimento_noticias",
    }

    for key, indicator in fuzzy_map.items():
        if key in nome_lower:
            return read_indicador_full(indicator)

    raise FileNotFoundError(f"Indicator '{nome}' not found")
