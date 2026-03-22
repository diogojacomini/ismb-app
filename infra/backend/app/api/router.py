"""
Aggregated API router — combines all domain sub-routers into one object
that is registered on the FastAPI application in main.py.

Adding a new domain:
  1. Create app/api/endpoints/my_domain.py with its own APIRouter.
  2. Import it here and call router.include_router(...).
"""

from fastapi import APIRouter

from .endpoints import (
    analytics,
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
