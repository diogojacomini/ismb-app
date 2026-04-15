"""
Correlation Analysis API Endpoint.

Fornece matriz de correlação entre os indicadores ISMB e indices de mercado.

Endpoints disponíveis:
    - GET /correlacao: Matriz de correlação consolidada
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from ...services import db_service

router = APIRouter(tags=["Análise"])


@router.get("/correlacao", summary="Matriz de correlação entre indicadores")
def get_correlacao():
    """
    Retorna matriz de correlação entre indicadores e indices.

    Calcula correlação entre:
    - Indice ISMB e componentes
    - indices de mercado (IBOV, IFIX, IVVB, CDS)

    Returns:
        JSONResponse: Lista de pares com correlações calculadas

    Raises:
        HTTPException 404: Dados de correlação não encontrados

    Example:
        GET /api/correlacao

        Response:
        [
            {
                "dat_ref": "2026-03-19",
                "entity_a": "ISMB",
                "entity_b": "IBOV",
                "correlation": 0.75,
                "n_obs": 180
            }
        ]
    """
    try:
        data = db_service.read_correlacao()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JSONResponse(content=data)
