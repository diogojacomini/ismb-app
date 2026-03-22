"""
Endpoint: /correlacao
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from ...services import csv_service

router = APIRouter(tags=["Análise"])


@router.get("/correlacao", summary="Matriz de correlação entre indicadores")
def get_correlacao():
    try:
        data = csv_service.read_correlacao()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JSONResponse(content=data)
