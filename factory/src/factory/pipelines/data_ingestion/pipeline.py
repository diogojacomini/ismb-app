"""
This is a boilerplate pipeline 'data_ingestion'
generated using Kedro 0.19.14
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import extract_transform_html_table, extract_transform_api_yf

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=extract_transform_html_table,
                inputs=["params:cds_parms", "params:columns_order"],
                outputs="rw_cds_stage",
                name="etl_html_cds_node",
            ),
            node(
                func=extract_transform_api_yf,
                inputs=["params:ibov_ticker"],
                outputs="rw_ibov_stage",
                name="etl_ibov_node",
            ),
        ]
    )
