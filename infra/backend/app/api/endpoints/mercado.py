"""
Endpoint: /mercado
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ...services import csv_service

router = APIRouter(tags=["Mercado"])


@router.get("/mercado", summary="Transações de mercado (IBOV, IFIX, IVVB, CDS)")
def get_mercado(
    days: Optional[int] = Query(90, ge=1, description="Número de dias de negociação a retornar")
):
    try:
        rows = csv_service.read_mercado()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    sorted_rows = sorted(rows, key=lambda r: (r.get("dat_ref", ""), r.get("cod_indice", "")))

    if days:
        all_dates = sorted({r.get("dat_ref") for r in sorted_rows if r.get("dat_ref")})
        cutoff_dates = set(all_dates[-days:])
        sorted_rows = [r for r in sorted_rows if r.get("dat_ref") in cutoff_dates]

    return JSONResponse(content=sorted_rows)
