"""
ISMB API - FastAPI Application Factory.

Aplicação principal que expõe a API REST do índice ISMB.
Gerencia lifecycle (inicialização de banco, cache, shutdown) e registra todos os routers de endpoints.

Documentação interativa disponível em:
    - Swagger UI: http://localhost:8000/docs
    - ReDoc: http://localhost:8000/redoc

uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .api.router import router as api_router
from .core import cache
from .core.config import DatabaseConfig
from .core.database import db_pool
from .services import db_service


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """
    Gerenciador de ciclo de vida da aplicação.

    Startup:
        1. Inicializa pool de conexões PostgreSQL
        2. Pre-carrega queries pesadas no cache (read_indice, read_mercado) para garantir primeira requisição instantânea

    Shutdown:
        1. Fecha todas as conexões do pool
        2. Limpa cache em memória

    Yields:
        Controle para o FastAPI executar requests
    """

    db_pool.initialize(
        DatabaseConfig.CONNECTION_STRING,
        minconn=DatabaseConfig.MIN_CONNECTIONS,
        maxconn=DatabaseConfig.MAX_CONNECTIONS,
    )

    for loader in (db_service.read_indice, db_service.read_mercado):
        try:
            loader()
        except Exception:
            pass

    yield

    # Cleanup on shutdown
    db_pool.close()
    cache.clear()


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
