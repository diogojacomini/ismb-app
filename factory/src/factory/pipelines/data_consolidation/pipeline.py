"""
This is a boilerplate pipeline 'data_ingestion'
generated using Kedro 0.19.14

Pipeline de consolidação dos dados brutos em tabelas fato.

Consolidações realizadas:
    - Transações de mercado: Une CDS, Ibovespa, IVVB11 e IFIX em uma unica tabela.
    - Noticias financeiras: Combina feeds de InfoMoney, Valor Investe, Seu Dinheiro e Money Times

As tabelas consolidadas servem como entrada para os modelos de analise de
sentimento e calculo dos indicadores.

"""
from kedro.pipeline import node, Pipeline, pipeline
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
