"""
This is a boilerplate pipeline 'data_quality'
generated using Kedro 0.19.14
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import (
    validate_stage_mercado
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=validate_stage_mercado,
                inputs=[
                    "stage_cds",
                    "params:dataset_name_cds",
                    "params:quality_thresholds",
                    "parameters"
                ],
                outputs="metrics_stage_quality",
                name="data_quality_validation_node",
                tags=["data_quality", "stage", "cds"],
            )
        ]
    )
