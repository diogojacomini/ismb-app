"""
Endpoints: /indicador_full/{nome}   /indicador/{nome}  (legacy)
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ...services import csv_service

router = APIRouter(tags=["Indicadores"])


@router.get("/indicador_full/{nome}", summary="Série temporal completa de um indicador")
def get_indicador_full(
    nome: str,
    days: Optional[int] = Query(365, ge=1, description="Número de dias a retornar"),
):
    try:
        rows = csv_service.read_indicador_full(nome)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Indicador '{nome}' não encontrado") from exc

    sorted_rows = sorted(rows, key=lambda r: r.get("dat_ref", ""))
    if days and len(sorted_rows) > days:
        sorted_rows = sorted_rows[-days:]
    return JSONResponse(content=sorted_rows)


@router.get("/indicador/{nome}", summary="Busca de indicador na pasta analytics (legado)")
def get_indicador(nome: str):
    """Legacy fuzzy-search endpoint kept for backwards compatibility."""
    try:
        data = csv_service.get_indicador(nome)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Indicador '{nome}' não encontrado") from exc
    return JSONResponse(content=data)
