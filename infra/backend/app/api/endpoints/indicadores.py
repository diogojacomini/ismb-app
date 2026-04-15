"""
Indicators API Endpoints.

Fornece acesso as séries temporais completas dos indicadores individuais que compõem o indice ISMB.

Endpoints disponíveis:
    - GET /indicador_full/{nome}: Série temporal completa de um indicador
    - GET /indicador/{nome}: Endpoint legado (mantido para compatibilidade)
"""

from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from ...services import db_service

router = APIRouter(tags=["Indicadores"])


@router.get("/indicador_full/{nome}", summary="Série temporal completa de um indicador")
def get_indicador_full(
    nome: str,
    days: Optional[int] = Query(365, ge=1, description="Número de dias a retornar"),
):
    """
    Retorna série temporal completa de um indicador específico.

    Indicadores disponíveis:
    - risco_credito: Apetite por risco baseado em spread e CDS
    - retorno_mercado: Performance do IBOV vs histórico
    - volatilidade_mercado: Medida de oscilação de preços
    - atividade_mercado: Volume de negociação
    - confianca_mercado_local: Confiança baseada em indices locais
    - sentimento_noticias: Análise de sentimento de mídia financeira

    Args:
        nome: Nome do indicador
        days: Número de dias a retornar (padrão: 365)

    Returns:
        JSONResponse: Lista de observações ordenadas por data

    Raises:
        HTTPException 404: Indicador não encontrado

    Example:
        GET /api/indicador_full/risco_credito?days=90

        Response:
        [
            {
                "dat_ref": "2026-03-19",
                "valor": 57.95,
                "componente_a": 1.2,
                "componente_b": 0.8
            }
        ]
    """
    try:
        rows = db_service.read_indicador_full(nome)
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=404, detail=f"Indicador '{nome}' não encontrado"
        ) from exc

    sorted_rows = sorted(rows, key=lambda r: r.get("dat_ref", ""))
    if days and len(sorted_rows) > days:
        sorted_rows = sorted_rows[-days:]
    return JSONResponse(content=sorted_rows)
