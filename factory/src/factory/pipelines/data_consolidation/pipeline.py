"""
This is a boilerplate pipeline 'data_consolidation'
generated using Kedro 0.19.14
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import (
    consolidate_data_transacoes,
    consolidate_data_noticias,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=consolidate_data_transacoes,
                inputs=[
                    "stage_cds",
                    "stage_ibov",
                    "stage_ivvb",
                    "stage_ifix",
                    "parameters",
                ],
                outputs="fato_transacao_mercado",
                name="data_consolidated_transacoes_node",
                tags=["pipeline-data_consolidation"],
            ),
            
            node(
                func=consolidate_data_noticias,
                inputs=[
                    "stage_infomoney",
                    "stage_valorinveste",
                    "stage_seudinheiro",
                    "stage_moneytimes",
                    "parameters",
                ],
                outputs="fato_transacao_noticias",
                name="data_consolidated_noticias_node",
                tags=["pipeline-data_consolidation"],
            ),
            
        ]
    )
