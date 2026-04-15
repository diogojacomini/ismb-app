"""Hooks de monitoramento que registram tempo de execucao e erros de pipelines e nodes."""
import logging
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from .saiph.monitoring import Monitor


logger = logging.getLogger(__name__)


class MonitoringHooks:
    """Hooks que integram o Monitor em cada pipeline e node do Kedro."""

    def __init__(self) -> None:
        self.monitors: dict = {}

    @hook_impl
    def before_pipeline_run(self, run_params: dict, pipeline, catalog: DataCatalog) -> None:
        """Inicia o monitor do pipeline antes da execucao."""
        pipeline_name = run_params.get("pipeline_name", "default")

        monitor = Monitor(pipeline_name, catalog=catalog)
        monitor.start(run_params, entity_type="pipeline")
        self.monitors[pipeline_name] = monitor

    @hook_impl
    def after_pipeline_run(self, run_params: dict, pipeline, catalog: DataCatalog) -> None:
        """Registra o pipeline como concluido com sucesso."""
        pipeline_name = run_params.get("pipeline_name", "default")
        extra_params = run_params.get("extra_params", {})

        monitor = self.monitors.get(pipeline_name)

        if monitor:
            monitor.end(status="SUCCESS", params=extra_params)

    @hook_impl
    def on_pipeline_error(self, error: Exception, run_params: dict, pipeline, catalog) -> None:
        """Registra erro no monitor e encerra o pipeline com status FAILED."""
        pipeline_name = run_params.get("pipeline_name", "default")
        extra_params = run_params.get("extra_params", {})

        monitor = self.monitors.get(pipeline_name)

        if monitor:
            monitor.set_error(str(error))
            monitor.end(status="FAILED", params=extra_params, entity_type="pipeline")

    @hook_impl
    def before_node_run(self, node, catalog, inputs: dict, is_async: bool) -> None:
        """Inicia o monitor do node antes da execucao."""
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
    def after_node_run(self, node, catalog, inputs: dict, outputs, is_async: bool) -> None:
        """Registra o node como concluido com sucesso."""
        extra_params = inputs.get('parameters')

        monitor = self.monitors.get(node.name)

        if monitor:
            monitor.end(status="SUCCESS", params=extra_params, entity_type="node")

    @hook_impl
    def on_node_error(self, error: Exception, node, catalog, inputs: dict, is_async: bool) -> None:
        """Registra erro no monitor e encerra o node com status FAILED."""
        extra_params = inputs.get('parameters')

        monitor = self.monitors.get(node.name)

        if monitor:
            monitor.set_error(str(error))
            monitor.end(status="FAILED", params=extra_params, entity_type="node")
