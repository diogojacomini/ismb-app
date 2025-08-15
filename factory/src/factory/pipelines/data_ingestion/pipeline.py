"""
This is a boilerplate pipeline 'data_ingestion'
generated using Kedro 0.19.14
"""
from kedro.pipeline import node, Pipeline, pipeline
from .nodes import (
    extract_transform_html_table,
    extract_transform_api_yf,
    extract_transform_infomoney,
    extract_transform_valorinveste,
    extract_transform_seudinheiro,
    extract_transform_moneytimes,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Pipeline principal de extração e transformação."""
    return pipeline(
        [
            node(
                func=extract_transform_html_table,
                inputs=["params:cds_parms", "params:columns_order", "parameters"],
                outputs="rw_cds_stage",
                name="etl_html_cds_node",
            ),
            node(
                func=extract_transform_api_yf,
                inputs=["params:ibov_ticker", "params:columns_mapping_yf", "parameters"],
                outputs="rw_ibov_stage",
                name="etl_ibov_node",
            ),
            node(
                func=extract_transform_api_yf,
                inputs=["params:ivvb11_ticker", "params:columns_mapping_yf", "parameters"],
                outputs="rw_ivvb_stage",
                name="etl_ivvb11_vix_brasil_node",
            ),
            node(
                func=extract_transform_html_table,
                inputs=["params:ifix_parms", "params:columns_order_ifix", "parameters"],
                outputs="rw_ifix_stage",
                name="etl_html_ifix_node",
            ),
            node(
                func=extract_transform_infomoney,
                inputs=["params:infomoney_parms", "parameters"],
                outputs="rw_infomoney_stage",
                name="etl_html_infomoney_node",
            ),
            node(
                func=extract_transform_valorinveste,
                inputs=["params:valorinveste_parms", "parameters"],
                outputs="rw_valorinveste_stage",
                name="etl_html_valorinveste_node",
            ),
            node(
                func=extract_transform_seudinheiro,
                inputs=["params:seudinheiro_parms", "parameters"],
                outputs="rw_seudinheiro_stage",
                name="etl_html_seudinheiro_node",
            ),
            node(
                func=extract_transform_moneytimes,
                inputs=["params:moneytimes_parms", "parameters"],
                outputs="rw_moneytimes_stage",
                name="etl_html_moneytimes_node",
            ),
        ]
    )
