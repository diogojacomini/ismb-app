"""
Data Quality API Endpoint.

Fornece relatórios de qualidade e validação dos dados processados pelos pipelines.

Endpoints disponíveis:
    - GET /quality: Relatório consolidado de qualidade
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from ...services import db_service

router = APIRouter(tags=["Governança"])


@router.get("/quality", summary="Relatório de qualidade de dados")
def get_quality():
    """
    Retorna relatório consolidado de qualidade das tabelas.

    Inclui validações de:
    - Completude: valores nulos, registros faltantes
    - Consistência: ranges esperados, outliers
    - Integridade: duplicatas, chaves únicas

    Returns:
        JSONResponse: Lista de relatórios de validação por dataset

    Raises:
        HTTPException 500: Erro ao gerar relatório de qualidade

    Example:
        GET /api/quality

        Response:
        [
            {
                "dataset": "fato_indice_ismb",
                "dat_ref": "2026-03-19",
                "ok": true,
                "n_rows": 2022,
                "null_count": 0,
                "out_of_range": 0,
                "note": "Validação bem-sucedida"
            }
        ]
    """
    try:
        data = db_service.read_quality()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return JSONResponse(content=data)
