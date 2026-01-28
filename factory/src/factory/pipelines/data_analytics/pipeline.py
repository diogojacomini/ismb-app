"""
This is a boilerplate pipeline 'data_analytics'
generated using Kedro 0.19.14
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import (
    build_dash_diario,
    build_serie_temporal_ismb,
    build_analise_correlacao,
    build_kpis_agregados
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=build_dash_diario,
                inputs=[
                    "fato_transacao_mercado",
                    "dim_tempo",
                    "dim_indice",
                    "fato_indice_ismb",
                    "parameters",
                    ],
                outputs="analytics_dashboard_diario",
                name="build_dashboard_diario_node",
                tags=["pipeline-data_analytics"],
            ),
            
            node(
                func=build_serie_temporal_ismb,
                inputs=[
                    "fato_indice_ismb",
                    "dim_tempo",
                    "parameters",
                    ],
                outputs="analytics_serie_temporal_ismb",
                name="build_serie_temporal_node",
                tags=["pipeline-data_analytics"],
            ),

            node(
                func=build_analise_correlacao,
                inputs=[
                    "fato_transacao_mercado",
                    "dim_tempo",
                    "dim_indice",
                    "fato_indice_ismb",
                    "parameters",
                    ],
                outputs="analytics_correlacao",
                name="build_analise_correlacao_node",
                tags=["pipeline-data_analytics"],
            ),

            node(
                func=build_kpis_agregados,
                inputs=[
                    "fato_indice_ismb",
                    "fato_transacao_mercado",
                    "dim_tempo",
                    "parameters",
                    ],
                outputs="analytics_kpis_agregados",
                name="build_kpis_agregados_node",
                tags=["pipeline-data_analytics"],
            ),
        ]
    )
