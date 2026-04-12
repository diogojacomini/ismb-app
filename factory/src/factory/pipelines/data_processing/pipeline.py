"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.14

Pipeline de processamento e cálculo dos indicadores. São seis indicadores que capturam
diferentes aspectos dosentimento e comportamento do mercado brasileiro.

Indicadores:
    1. Risco de crédito: Baseado no spread do CDS Brasil 5 anos
    2. Retorno de mercado: Performance média dos índices Ibovespa e IVVB11
    3. Volatilidade: Medida de oscilação dos preços no período
    4. Atividade de mercado: Volume negociado e liquidez
    5. Confiança no mercado local: Comportamento do IFIX
    6. Sentimento da mídia: Análise de notícias financeiras via LLM

"""
from kedro.pipeline import node, Pipeline, pipeline
from .nodes import (
    indicador_risco_credito,
    indicador_retorno_mercado,
    indicador_volatilidade_mercado,
    indicador_atividade_mercado,
    indicador_confianca_mercado_local,
    indicador_sentimento_midia,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=indicador_risco_credito,
                inputs=[
                    "fato_transacao_mercado",
                    "dim_tempo",
                    "params:parameters_indicador_risco_credito",
                    "parameters",
                ],
                outputs="indicador_risco_credito",
                name="indicador_risco_credito_node",
                tags=["pipeline-calculo_indicadores"],
            ),
            node(
                func=indicador_retorno_mercado,
                inputs=[
                    "fato_transacao_mercado",
                    "dim_tempo",
                    "params:parameters_retorno_mercado",
                    "parameters",
                ],
                outputs="indicador_retorno_mercado",
                name="indicador_retorno_mercado_node",
                tags=["pipeline-calculo_indicadores"],
            ),
            node(
                func=indicador_volatilidade_mercado,
                inputs=[
                    "fato_transacao_mercado",
                    "dim_tempo",
                    "params:parameters_volatilidade_mercado",
                    "parameters",
                ],
                outputs="indicador_volatilidade_mercado",
                name="indicador_volatilidade_mercado_node",
                tags=["pipeline-calculo_indicadores"],
            ),
            node(
                func=indicador_atividade_mercado,
                inputs=[
                    "fato_transacao_mercado",
                    "dim_tempo",
                    "params:parameters_atividade_mercado",
                    "parameters",
                ],
                outputs="indicador_atividade_mercado",
                name="indicador_atividade_mercado_node",
                tags=["pipeline-calculo_indicadores"],
            ),
            node(
                func=indicador_confianca_mercado_local,
                inputs=[
                    "fato_transacao_mercado",
                    "dim_tempo",
                    "params:parameters_confianca_mercado",
                    "parameters",
                ],
                outputs="indicador_confianca_mercado_local",
                name="indicador_confianca_mercado_local_node",
                tags=["pipeline-calculo_indicadores"],
            ),
            node(
                func=indicador_sentimento_midia,
                inputs=[
                    "fato_transacao_noticias",
                    "params:parameters_sentimento_midia",
                    "parameters",
                ],
                outputs="indicador_sentimento_noticias",
                name="indicador_sentimento_noticias_node",
                tags=["pipeline-calculo_indicadores"],
            ),
        ]
    )
