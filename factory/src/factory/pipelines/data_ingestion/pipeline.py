"""
This is a boilerplate pipeline 'data_ingestion'
generated using Kedro 0.19.14
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import extract_transform_html_table

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=extract_transform_html_table,
                inputs=["params:cds_parms", "params:columns_order"],
                outputs="tb_cds",
                name="etl_html_cds_node",
            ),
        ]
    )
