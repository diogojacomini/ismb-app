"""
Data API Endpoint.

Fornece dados de mercado (preços, volumes, variações).

Endpoints disponíveis:
    - GET /mercado: Transações de mercado dos indices
"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from ...services import db_service

router = APIRouter(tags=["Mercado"])


@router.get("/mercado", summary="Transações de mercado (IBOV, IFIX, IVVB, CDS)")
def get_mercado(
    days: Optional[int] = Query(
        90, ge=1, description="Número de dias de negociação a retornar"
    )
):
    """
    Retorna dados diários de mercado.

    Inclui:
    - IBOV: Índice Bovespa (ações)
    - IFIX: Índice de Fundos Imobiliários
    - IVVB: Índice Small Caps
    - CDS: Credit Default Swap Brasil 5Y

    Dados incluem: abertura, fechamento, máxima, mínima e volume.

    Args:
        days: Número de dias de negociação a retornar (padrão: 90)

    Returns:
        JSONResponse: Lista de transações ordenadas por data e índice

    Raises:
        HTTPException 404: Dados de mercado não encontrados

    Example:
        GET /api/mercado?days=30

        Response:
        [
            {
                "dat_ref": "2026-03-19",
                "cod_indice": "IBOV",
                "val_fechamento": 180271.0,
                "val_abertura": 179624.0,
                "val_minima": 176296.0,
                "val_maxima": 181251.0,
                "qtd_volume": 12560400
            }
        ]
    """
    try:
        rows = db_service.read_mercado()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    sorted_rows = sorted(
        rows, key=lambda r: (r.get("dat_ref", ""), r.get("cod_indice", ""))
    )

    if days:
        all_dates = sorted({r.get("dat_ref") for r in sorted_rows if r.get("dat_ref")})
        cutoff_dates = set(all_dates[-days:])
        sorted_rows = [r for r in sorted_rows if r.get("dat_ref") in cutoff_dates]

    return JSONResponse(content=sorted_rows)
