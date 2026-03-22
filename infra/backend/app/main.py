"""
ISMB API — FastAPI application factory.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.router import router as api_router
from .core import cache
from .services import csv_service


# ── lifespan: pre-warm cache on startup ───────────────────────────────────

@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Pre-load the two heaviest CSVs so the first real request is instant."""
    for loader in (csv_service.read_indice, csv_service.read_mercado):
        try:
            loader()
        except FileNotFoundError:
            pass  # data may not exist in all environments
    yield
    cache.clear()


# ── application ────────────────────────────────────────────────────────────

app = FastAPI(
    title="ISMB API",
    description="API de dados do Índice de Sentimento do Mercado Brasileiro.",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api")
