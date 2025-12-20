"""
Hooks personalizados para particionamento por data
"""
import logging
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from .saiph.monitoring import Monitor


logger = logging.getLogger(__name__)

class MonitoringHooks:
    """
    Hooks para monitoramento automático de pipelines.

    Rastreia:
        - Tempo de execução de cada pipeline e node.
        - Custos estimados
        - Data Lineage
        - Erros e exceções
    """

    def __init__(self):
        self.monitors = {}
        self.node_start_times = {}

    @hook_impl
    def before_pipeline_run(self, run_params, pipeline, catalog: DataCatalog):
        """
        Executado antes de cada pipeline inicar.
        """
        pipeline_name = run_params.get("pipeline_name", "default")

        monitor = Monitor(pipeline_name, catalog=catalog)
        monitor.start(run_params)
        self.monitors[pipeline_name] = monitor
    
    @hook_impl
    def after_pipeline_run(self, run_params, pipeline, catalog: DataCatalog):
        pipeline_name = run_params.get("pipeline_name", "default")
        extra_params = run_params.get("extra_params", {})

        monitor = self.monitors.get(pipeline_name)

        if monitor:
            result = monitor.end(status="SUCCESS", params=extra_params)

    @hook_impl
    def on_pipeline_error(self, error, run_params, pipeline, catalog):
        pass

    @hook_impl
    def before_node_run(self, node, catalog, inputs, is_async):
        pass

    @hook_impl
    def after_node_run(self, node, catalog, inputs, outputs, is_async):
        pass
    
    @hook_impl
    def on_node_error(self, error, node, catalog, inputs, is_async):
        pass
