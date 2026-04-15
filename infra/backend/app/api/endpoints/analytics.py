"""
Analytics API Endpoints.

Acesso aos dados consolidados de analytics, incluindo as KPIs agregados para o dashboard diario.

Endpoints disponiveis:
    - GET /analytics: Lista recursos disponiveis
    - GET /analytics/{filename}: Lê arquivo analytics específico
    - GET /kpis: Retorna KPIs agregados por ano e entidade
    - GET /dashboard_diario: Dashboard diario com métricas consolidadas
"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from ...services import db_service

router = APIRouter(tags=["Analytics"])


@router.get("/analytics", summary="Lista as tabelas disponiveis de analytics")
def list_analytics():
    """
    Lista os recursos disponiveis.

    Returns:
        JSONResponse: Lista de nomes de tabelas de analytics disponiveis.

    Example:
        GET /api/analytics

        Response:
        [
            "analytics_kpis_agregados",
            "analytics_dashboard_diario",
            "analytics_correlacao"
        ]
    """
    return JSONResponse(content=db_service.list_analytics())


@router.get("/analytics/{filename}", summary="Lê uma tabela de analytics")
def read_analytics(filename: str):
    """
    Lê o conteúdo da tabela de analytics.

    Args:
        filename: Nome da tabela analytics (ex: 'analytics_kpis_agregados')

    Returns:
        JSONResponse: Conteúdo da tabela como lista de dicionários.

    Raises:
        HTTPException 404: Tabela não encontrado.

    Example:
        GET /api/analytics/analytics_kpis_agregados

        Response:
        [
            {
                "dat_ref": "2026-03-19",
                "entity": "IBOV",
                "year": 2026,
                "return": 12.5,
                "volatility": 18.3
            }
        ]
    """
    try:
        data = db_service.read_analytics(filename)
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=404, detail=f"Tabela '{filename}' não encontrado"
        ) from exc
    return JSONResponse(content=data)


@router.get("/kpis", summary="KPIs agregados (analytics_kpis_agregados)")
def get_kpis():
    """
    Retorna KPIs agregados por ano e indice de mercado.

    Inclui métricas de desempenho anualizadas como retorno, voatilidade,
    drawdown máximo e outras estatísticas para ISMB, IBOV, IFIX e IVVB.

    Returns:
        JSONResponse: Lista de KPIs com métricas anualizadas.

    Raises:
        HTTPException 404: Dados de KPIs não encontrados.

    Example:
        GET /api/kpis

        Response:
        [
            {
                "dat_ref": "2026-03-19",
                "entity": "ISMB",
                "year": 2026,
                "return": 8.5,
                "volatility": 12.3,
                "max_drawdown": -5.2,
                "n_obs": 65
            }
        ]
    """
    try:
        data = db_service.read_kpis()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JSONResponse(content=data)


@router.get("/dashboard_diario", summary="Dashboard diario por indice")
def get_dashboard_diario(days: Optional[int] = Query(30, ge=1, description="Número de dias a retornar")):
    """
    Retorna dados consolidados do dashboard diario por indice.

    Métricas diarias para cada indice de mercado (IBOV, IFIX, IVVB, CDS).

    Args:
        days: Número de dias a retornar (padrão: 30, mínimo: 1).

    Returns:
        JSONResponse: Lista de registros diarios ordenados por data e indice.

    Raises:
        HTTPException 404: Dados de dashboard não encontrados.

    Example:
        GET /api/dashboard_diario?days=7

        Response:
        [
            {
                "dat_ref": "2026-03-19",
                "cod_indice": "IBOV",
                "val_fechamento": 180271.0,
                "variacao_dia": 0.35,
                "volume": 12560400,
                "sma_20": 179500.5,
                "volatility_20d": 1.8
            }
        ]
    """
    try:
        rows = db_service.read_dashboard_diario()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    # Ordena por data e indice para garantir consistência
    sorted_rows = sorted(
        rows, key=lambda r: (r.get("dat_ref", ""), r.get("cod_indice", ""))
    )

    # Filtra últimos N dias se especificado
    if days:
        all_dates = sorted({r.get("dat_ref") for r in sorted_rows if r.get("dat_ref")})
        cutoff_dates = set(all_dates[-days:])
        sorted_rows = [r for r in sorted_rows if r.get("dat_ref") in cutoff_dates]

    return JSONResponse(content=sorted_rows)
