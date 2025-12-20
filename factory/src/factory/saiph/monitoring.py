import logging
import time
from datetime import datetime
from typing import Any
from pandas import DataFrame

logger = logging.getLogger(__name__)


class Monitor:
    def __init__(self, pipeline_name: str, catalog=None):
        self.pipeline_name = pipeline_name
        self.start_time = None
        self.end_time = None
        self.metrics = {}
        self.errors = []
        self.catalog = catalog

    def start(self):
        self.start_time = time.time()
        logger.info(f"[MONITOR] Pipeline '{self.pipeline_name}' started.")
    
    def end(self, status: str = "SUCCESS", params: dict = None):
        self.end_time = time.time()
        duration = self.end_time - self.start_time

        logger.info(f"[MONITOR] Pipeline '{self.pipeline_name}' ended with status '{status}' in {duration:.2f} seconds.")
        result = {
            'pipeline': self.pipeline_name,
            'status': status,
            'duration_seconds': duration,
            'start_time': datetime.fromtimestamp(self.start_time).isoformat(),
            'end_time': datetime.fromtimestamp(self.end_time).isoformat(),
            'metrics': self.metrics,
            'errors': self.errors,
            'dat_ref': params.get('odate') if params else None,
            'environment': 'prd' if params.get('environment') is None else params.get('environment'),
            'process_full_data': False if params.get('process_full_data') is None else params.get('process_full_data')

        }

        self._save_to_catalog(result)

        return result
    
    def set_metric(self, key: str, value: Any):
        self.metrics[key] = value
    
    def set_error(self, error: Exception):
        self.errors.append({
            'error': error,
            'timestamp': datetime.now().isoformat()
            })

    def _save_to_catalog(self, result: dict):
        try:
            if self.catalog is not None:
                df = DataFrame([result])
                self.catalog.save("pipeline_metrics", df)
                logger.info(f"[MONITOR] Metrics for pipeline '{self.pipeline_name}' saved to catalog.")
            else:
                logger.warning(f"[MONITOR] ERROR '{self.pipeline_name}'.")
        except Exception as e:
            logger.error(f"[MONITOR] Failed to save metrics to catalog: {e}")
