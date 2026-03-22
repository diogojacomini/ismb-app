"""
Endpoints: /analytics   /analytics/{filename}
           /kpis         /dashboard_diario
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ...services import csv_service

router = APIRouter(tags=["Analytics"])


@router.get("/analytics", summary="Lista arquivos CSV disponíveis na pasta analytics")
def list_analytics():
    return JSONResponse(content=csv_service.list_analytics())


@router.get("/analytics/{filename}", summary="Lê um arquivo CSV da pasta analytics")
def read_analytics(filename: str):
    try:
        data = csv_service.read_analytics(filename)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Arquivo '{filename}' não encontrado") from exc
    return JSONResponse(content=data)


@router.get("/kpis", summary="KPIs agregados (analytics_kpis_agregados.csv)")
def get_kpis():
    try:
        data = csv_service.read_kpis()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JSONResponse(content=data)


@router.get("/dashboard_diario", summary="Dashboard diário por índice")
def get_dashboard_diario(
    days: Optional[int] = Query(30, ge=1, description="Número de dias a retornar")
):
    try:
        rows = csv_service.read_dashboard_diario()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    sorted_rows = sorted(rows, key=lambda r: (r.get("dat_ref", ""), r.get("cod_indice", "")))

    if days:
        all_dates = sorted({r.get("dat_ref") for r in sorted_rows if r.get("dat_ref")})
        cutoff_dates = set(all_dates[-days:])
        sorted_rows = [r for r in sorted_rows if r.get("dat_ref") in cutoff_dates]

    return JSONResponse(content=sorted_rows)
