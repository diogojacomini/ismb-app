"""Monitoramento de execucao de pipelines e nodes Kedro.

Registra inicio, fim, duracao, parametros e erros de cada
pipeline ou node. Os resultados sao gravados no catalogo
como o dataset 'pipeline_logs'.
"""
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any

from pandas import DataFrame

logger = logging.getLogger(__name__)


class Monitor:
    """Rastreia execucao de um pipeline ou node.

    Registra tempo, parametros e erros, e grava o resultado
    no catalogo ao final da execucao.
    """

    def __init__(self, entity_name: str, catalog: Any = None) -> None:
        self.entity_name = entity_name
        self.start_time: float | None = None
        self.end_time: float | None = None
        self.metrics: dict = {}
        self.errors: list = []
        self.catalog = catalog
        self.run_id: str | None = None
        self.params: dict = {}

    def start(self, params: dict, entity_type: str = "pipeline") -> None:
        """Inicia o rastreamento. Registra run_id, hora de inicio e parametros."""
        self.start_time = time.time()
        self.run_id = uuid.uuid4().hex[:8]
        self.params = params
        start_iso = datetime.fromtimestamp(self.start_time).isoformat()
        extra = self.params.get("extra_params", {})

        banner = [
            "=" * 80,
            "| STARTING %s: %s" % (entity_type.upper(), self.entity_name),
            "-" * 80,
            " run_id                 : %s" % self.run_id,
            " start_time             : %s" % start_iso,
            " parm odate             : %s" % json.dumps(extra.get("odate")),
            " parm environment       : %s" % json.dumps(extra.get("environment", "prd")),
            " parm process_full_data : %s" % json.dumps(extra.get("process_full_data", False)),
        ]

        if entity_type.upper() == "NODE":
            banner.extend([
                " node.inputs            : %s" % json.dumps(extra.get("node.inputs", [])),
                " node.outputs           : %s" % json.dumps(extra.get("node.outputs", [])),
            ])

        banner.append("=" * 80)
        for line in banner:
            logger.info(line)

    def end(
        self,
        status: str = "SUCCESS",
        params: dict | None = None,
        entity_type: str = "pipeline",
    ) -> dict:
        """Finaliza o rastreamento, calcula duracao e grava no catalogo."""
        self.end_time = time.time()
        duration = self.end_time - self.start_time  # type: ignore[operator]
        params = params or {}

        logger.info(
            "[MONITOR] %s '%s' ended with status '%s' in %.2f seconds.",
            entity_type.title(), self.entity_name, status, duration,
        )

        result = {
            "run_id": self.run_id,
            "entity_name": self.entity_name,
            "entity_type": entity_type,
            "status": status,
            "duration_seconds": round(duration, 2),
            "start_time": datetime.fromtimestamp(self.start_time).isoformat(),  # type: ignore[arg-type]
            "end_time": datetime.fromtimestamp(self.end_time).isoformat(),
            "metrics": self.metrics,
            "errors": self.errors,
            "dat_ref": params.get("odate"),
            "environment": params.get("environment", "prd"),
            "process_full_data": params.get("process_full_data", False),
        }

        self._save_to_catalog(result)
        return result

    def set_metric(self, key: str, value: Any) -> None:
        """Registra uma metrica de execucao."""
        self.metrics[key] = value

    def set_error(self, error: str) -> None:
        """Registra um erro ocorrido durante a execucao."""
        self.errors.append({
            "error": error,
            "timestamp": datetime.now().isoformat(),
        })

    def _save_to_catalog(self, result: dict) -> None:
        """Persiste o resultado no dataset 'pipeline_logs' do catalogo."""
        try:
            if self.catalog is not None:
                df = DataFrame([result])
                self.catalog.save("pipeline_logs", df)
                logger.info("[MONITOR] Metrics for '%s' saved to catalog.", self.entity_name)
            else:
                logger.warning("[MONITOR] Catalog not available for '%s'.", self.entity_name)
        except Exception as e:
            logger.error("[MONITOR] Failed to save metrics to catalog: %s", e)
