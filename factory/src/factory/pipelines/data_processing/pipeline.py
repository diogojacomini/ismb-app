"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.14
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import (
    indicador_risco_credito,
    indicador_retorno_mercado,
    indicador_volatilidade_mercado,
    indicador_atividade_mercado,
    indicador_confianca_mercado_local,
    indicador_sentimento_midia
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=indicador_risco_credito,
                inputs=[
                    "rw_cds_stage",
                    "params:parameters_indicador_risco_credito",
                    "parameters"
                ],
                outputs="indicador_risco_credito",
                name="indicador_risco_credito_node",
            ),
            node(
                func=indicador_retorno_mercado,
                inputs=[
                    "rw_ibov_stage",
                    "params:parameters_retorno_mercado",
                    "parameters"
                ],
                outputs="indicador_retorno_mercado",
                name="indicador_retorno_mercado_node",
            ),
            node(
                func=indicador_volatilidade_mercado,
                inputs=[
                    "rw_ibov_stage",
                    "rw_ivvb_stage",
                    "params:parameters_volatilidade_mercado",
                    "parameters"
                ],
                outputs="indicador_volatilidade_mercado",
                name="indicador_volatilidade_mercado_node",
            ),
            node(
                func=indicador_atividade_mercado,
                inputs=[
                    "rw_ibov_stage",
                    "params:parameters_atividade_mercado",
                    "parameters"
                ],
                outputs="indicador_atividade_mercado",
                name="indicador_atividade_mercado_node",
            ),
            node(
                func=indicador_confianca_mercado_local,
                inputs=[
                    "rw_ifix_stage",
                    "params:parameters_confianca_mercado",
                    "parameters"
                ],
                outputs="indicador_confianca_mercado_local",
                name="indicador_confianca_mercado_local_node",
            ),
            node(
                func=indicador_sentimento_midia,
                inputs=[
                    "rw_infomoney_stage",
                    "rw_moneytimes_stage",
                    "rw_valorinveste_stage",
                    "rw_seudinheiro_stage",
                    "parameters"
                ],
                outputs="indicador_sentimento_noticias",
                name="indicador_sentimento_noticias_node",
            ),
        ]
    )
