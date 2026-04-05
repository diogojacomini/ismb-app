"""
API Router Aggregator.

Combina todos os routers de domínio (endpoints/*) em um único router.

Organização de endpoints por domínio:
    - indice: Índice ISMB principal e componentes
    - serie_temporal: Série ISMB com indicadores técnicos
    - correlacao: Matriz de correlação entre entidades
    - indicadores: Indicadores individuais detalhados
    - mercado: Dados de mercado (IBOV, IFIX, IVVB, CDS)
    - quality: Relatórios de qualidade de dados
    - analytics: KPIs e dashboards consolidados
    - cache: Endpoints administrativos de cache

"""
from fastapi import APIRouter
from .endpoints import (
    analytics,
    cache,
    correlacao,
    indicadores,
    indice,
    mercado,
    quality,
    serie_temporal,
)

router = APIRouter()

router.include_router(indice.router)
router.include_router(serie_temporal.router)
router.include_router(correlacao.router)
router.include_router(indicadores.router)
router.include_router(mercado.router)
router.include_router(quality.router)
router.include_router(analytics.router)
router.include_router(cache.router)
