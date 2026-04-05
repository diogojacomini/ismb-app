"""
Time Series API Endpoint.

Fornece série temporal completa do índice ISMB.

Endpoints disponíveis:
    - GET /serie_temporal: Série ISMB com indicadores técnicos
"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from ...services import db_service

router = APIRouter(tags=["Análise"])


@router.get("/serie_temporal", summary="Série temporal ISMB completa")
def get_serie_temporal(
    days: Optional[int] = Query(365, ge=1, description="Número de dias a retornar")
):
    """
    Retorna série temporal do ISMB com indicadores técnicos.

    Inclui:
    - Índice ISMB diário
    - Médias móveis (SMA 20, SMA 50)
    - Bandas de Bollinger
    - Volatilidade histórica
    - RSI (Relative Strength Index)

    Args:
        days: Número de dias a retornar (padrão: 365)

    Returns:
        JSONResponse: Lista de observações com indicadores

    Raises:
        HTTPException 404: Dados não encontrados

    Example:
        GET /api/serie_temporal?days=180

        Response:
        [
            {
                "dat_ref": "2026-03-19",
                "indice_ismb": 49.6,
                "sma_20": 51.2,
                "sma_50": 48.5,
                "bb_upper": 55.0,
                "bb_lower": 47.4,
                "volatility_20d": 2.3,
                "rsi_14": 48.5
            }
        ]
    """
    try:
        rows = db_service.read_serie_temporal()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    sorted_rows = sorted(rows, key=lambda r: r.get("dat_ref", ""))
    if days and len(sorted_rows) > days:
        sorted_rows = sorted_rows[-days:]
    return JSONResponse(content=sorted_rows)
