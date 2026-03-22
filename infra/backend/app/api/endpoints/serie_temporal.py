"""
Endpoint: /serie_temporal
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ...services import csv_service

router = APIRouter(tags=["Análise"])


@router.get("/serie_temporal", summary="Série temporal ISMB completa")
def get_serie_temporal(
    days: Optional[int] = Query(365, ge=1, description="Número de dias a retornar")
):
    try:
        rows = csv_service.read_serie_temporal()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    sorted_rows = sorted(rows, key=lambda r: r.get("dat_ref", ""))
    if days and len(sorted_rows) > days:
        sorted_rows = sorted_rows[-days:]
    return JSONResponse(content=sorted_rows)
