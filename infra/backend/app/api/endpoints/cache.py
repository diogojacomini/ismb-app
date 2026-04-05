"""
Cache Management API Endpoints.

Fornece endpoints para monitorar e gerenciar o cache da API,
permitindo limpeza manual após atualizações de dados.

Endpoints disponíveis:
    - POST /cache/clear: Limpa todo o cache
    - GET /cache/status: Estatísticas do cache
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from ...core import cache

router = APIRouter(tags=["Admin"])


@router.post("/cache/clear", summary="Limpar cache da API")
def clear_cache():
    """
    Limpa todo o cache da API.

    Deve ser executado após rodar pipelines Kedro para garantir que
    as próximas requisições retornem dados atualizados do banco.

    Usuário comum não precisa usar. Use apenas para atualizações imediatas.

    Returns:
        JSONResponse: Mensagem de confirmação

    Example:
        POST /api/cache/clear

        Response:
        {
            "status": "success",
            "message": "Cache limpo com sucesso..."
        }
    """
    cache.clear()
    return JSONResponse(
        content={
            "status": "success",
            "message": "Cache limpo com sucesso. Próximas requisições buscarão dados atualizados do banco.",
        }
    )


@router.get("/cache/status", summary="Status do cache")
def cache_status():
    """
    Retorna estatísticas sobre o estado atual do cache.

    Fornece métricas de uso do cache incluindo:
    - Número total de chaves armazenadas
    - Chaves ativas (não expiradas)
    - Chaves expiradas (aguardando limpeza automática)
    - TTL configurado

    Útil para monitorar performance e debug.

    Returns:
        JSONResponse: Estatísticas do cache

    Example:
        GET /api/cache/status

        Response:
        {
            "total_keys": 8,
            "live_keys": 8,
            "expired_keys": 0,
            "ttl_seconds": 60,
            "description": "Cache automático com TTL de 60 segundos"
        }
    """
    stats = cache.stats()
    return JSONResponse(
        content={
            "total_keys": stats["total_keys"],
            "live_keys": stats["live_keys"],
            "expired_keys": stats["expired_keys"],
            "ttl_seconds": 60,
            "description": "Cache automático com TTL de 60 segundos",
        }
    )
