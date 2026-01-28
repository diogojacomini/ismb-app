"""
This is a boilerplate pipeline 'data_validation'
generated using Kedro 0.19.14
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import (
    validate_mercado_consistency,
    validate_indicadores_consistency,
    validate_ismb_index,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            # Validação de consistência entre Dados de Mercado
            node(
                func=validate_mercado_consistency,
                inputs=[
                    "fato_transacao_mercado",
                    "dim_tempo",
                    "parameters",
                ],
                outputs="validation_data_mercado",
                name="validate_mercado_node",
                tags=["pipeline-data_validation"],
            ),

            # Validação de consistência entre Indicadores
            node(
                func=validate_indicadores_consistency,
                inputs=[
                    "indicador_risco_credito",
                    "indicador_retorno_mercado",
                    "indicador_volatilidade_mercado",
                    "indicador_atividade_mercado",
                    "indicador_confianca_mercado_local",
                    "indicador_sentimento_noticias",
                    "parameters",
                ],
                outputs="validation_data_indicadores",
                name="validate_indicadores_node",
                tags=["pipeline-data_validation"],
            ),
            
            # Validação do Indice ISMB Final
            node(
                func=validate_ismb_index,
                inputs=[
                    "fato_indice_ismb",
                    "parameters",
                ],
                outputs="validation_indice_isbm",
                name="validate_indice_isbm_node",
                tags=["pipeline-data_validation"],
            ),
        ]
    )
