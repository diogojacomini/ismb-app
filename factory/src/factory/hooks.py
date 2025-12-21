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
        monitor.start(run_params, entity_type="pipeline")
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
        pipeline_name = run_params.get("pipeline_name", "default")
        extra_params = run_params.get("extra_params", {})

        monitor = self.monitors.get(pipeline_name)

        if monitor:
            monitor.set_error(str(error))
            monitor.end(status="FAILED", params=extra_params, entity_type="pipeline")

    @hook_impl
    def before_node_run(self, node, catalog, inputs, is_async):
        monitor = Monitor(node.name, catalog=catalog)

        run_params = {
            'extra_params': {
                'odate': inputs.get('parameters').get('odate'),
                'environment': inputs.get('parameters').get('environment', 'prd'),
                'process_full_data': inputs.get('parameters').get('process_full_data', False),
                'node.inputs': node.inputs,
                'node.outputs': node.outputs
                }
            }

        monitor.start(run_params, entity_type="node")
        self.monitors[node.name] = monitor

    @hook_impl
    def after_node_run(self, node, catalog, inputs, outputs, is_async):
        extra_params = inputs.get('parameters')

        monitor = self.monitors.get(node.name)

        if monitor:
            result = monitor.end(status="SUCCESS", params=extra_params, entity_type="node")
    
    @hook_impl
    def on_node_error(self, error, node, catalog, inputs, is_async):
        extra_params = inputs.get('parameters')

        monitor = self.monitors.get(node.name)

        if monitor:
            monitor.set_error(str(error))
            monitor.end(status="FAILED", params=extra_params, entity_type="node")
