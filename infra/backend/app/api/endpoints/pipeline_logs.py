"""
Pipeline Logs API Endpoints.

Acesso aos logs de execução dos pipelines por node, para monitoramento de performance.

Endpoints disponíveis:
    - GET /pipeline_logs: Retorna métricas de execução dos nodes dos pipelines
"""
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from ...services import db_service

router = APIRouter(tags=["Pipeline Logs"])


@router.get("/pipeline_logs", summary="Métricas de execução dos pipelines por node")
def get_pipeline_logs():
    """
    Retorna logs de execução dos nodes dos pipelines.

    Filtra automaticamente por:
    - entity_type = 'node'
    - process_full_data = 'false'
    - environment = 'production'

    Returns:
        JSONResponse: Lista de logs com métricas de performance por node.

    Example:
        GET /api/pipeline_logs

        Response:
        [
            {
                "entity_name": "build_analytics_kpis",
                "entity_type": "node",
                "dat_ref_carga": "2026-03-30",
                "duration_time": 2.45,
                "status": "success",
                "process_full_data": "false",
                "environment": "production",
                "created_at": "2026-03-31T10:15:00"
            }
        ]
    """
    data = db_service.read_pipeline_logs()
    return JSONResponse(content=data)
