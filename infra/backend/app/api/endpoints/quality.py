"""
Endpoint: /quality
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from ...services import csv_service

router = APIRouter(tags=["Governança"])


@router.get("/quality", summary="Relatório de qualidade de dados")
def get_quality():
    try:
        data = csv_service.read_quality()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return JSONResponse(content=data)
