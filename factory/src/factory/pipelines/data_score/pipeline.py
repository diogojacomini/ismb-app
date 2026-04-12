"""
This is a boilerplate pipeline 'data_score' generated using Kedro 0.19.14

Pipeline de cálculo do indice ISMB, que representa o sentimento geral do mercado brasileiro.

O indice final:
    - 0-30: Sentimento muito negativo
    - 30-45: Sentimento negativo
    - 45-55: Sentimento neutro
    - 55-70: Sentimento positivo
    - 70-100: Sentimento muito positivo

"""
from kedro.pipeline import node, Pipeline, pipeline
from .nodes import calculate_score_dim


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=calculate_score_dim,
                inputs=[
                    "params:score_isbm",
                    "dim_tempo",
                    "dim_indicador",
                    "indicador_risco_credito",
                    "indicador_retorno_mercado",
                    "indicador_volatilidade_mercado",
                    "indicador_atividade_mercado",
                    "indicador_confianca_mercado_local",
                    "indicador_sentimento_noticias",
                    "parameters",
                ],
                outputs="fato_indice_ismb",
                name="process_score_data_node",
                tags=["pipeline-calculo_score"],
            ),
        ]
    )
