"""
This is a boilerplate pipeline 'data_score'
generated using Kedro 0.19.14
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import calculate_score_dim

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=calculate_score_dim,
                inputs=[
                    "params:score_isbm",
                    "indicador_risco_credito",
                    "indicador_retorno_mercado",
                    "indicador_volatilidade_mercado",
                    "indicador_atividade_mercado",
                    "indicador_confianca_mercado_local",
                    "indicador_sentimento_noticias",
                ],
                outputs="indice_isbm",
                name="process_score_data_node",
            ),
        ]
    )
