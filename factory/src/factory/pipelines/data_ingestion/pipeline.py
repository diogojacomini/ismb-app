"""
This is a boilerplate pipeline 'data_ingestion'
generated using Kedro 0.19.14

Extrai e transforma dados de mercado e noticias financeiras,
cada node é responsavel por uma fonte de dados.

Fontes de dados:
    Mercado:
        - CDS Brasil 5Y: web scraping de tabela HTML
        - Ibovespa: API yfinance
        - IVVB11 (VIX Brasil): API yfinance
        - IFIX: web scraping de tabela HTML

    Noticias:
        - InfoMoney: scraping
        - Valor Investe: scraping
        - Seu Dinheiro: scraping
        - Money Times: scraping

Fluxo de dados:
    1. Coleta dados da fonte externa
    2. Normalizacao de tipos (datas, numericos)
    5. Retorno de DataFrame pronto para gravacao mapeados no catalog.yml

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
                outputs="stage_cds",
                name="etl_html_cds_node",
                tags=["pipeline-ingestion", "cds"],
            ),
            node(
                func=extract_transform_api_yf,
                inputs=[
                    "params:ibov_ticker",
                    "params:columns_mapping_yf",
                    "parameters",
                ],
                outputs="stage_ibov",
                name="etl_ibov_node",
                tags=["pipeline-ingestion", "ibov"],
            ),
            node(
                func=extract_transform_api_yf,
                inputs=[
                    "params:ivvb11_ticker",
                    "params:columns_mapping_yf",
                    "parameters",
                ],
                outputs="stage_ivvb",
                name="etl_ivvb11_vix_brasil_node",
                tags=["pipeline-ingestion", "ivvb"],
            ),
            node(
                func=extract_transform_html_table,
                inputs=["params:ifix_parms", "params:columns_order_ifix", "parameters"],
                outputs="stage_ifix",
                name="etl_html_ifix_node",
                tags=["pipeline-ingestion", "ifix"],
            ),
            node(
                func=extract_transform_infomoney,
                inputs=["params:infomoney_parms", "parameters"],
                outputs="stage_infomoney",
                name="etl_html_infomoney_node",
                tags=["pipeline-ingestion", "noticias"],
            ),
            node(
                func=extract_transform_valorinveste,
                inputs=["params:valorinveste_parms", "parameters"],
                outputs="stage_valorinveste",
                name="etl_html_valorinveste_node",
                tags=["pipeline-ingestion", "noticias"],
            ),
            node(
                func=extract_transform_seudinheiro,
                inputs=["params:seudinheiro_parms", "parameters"],
                outputs="stage_seudinheiro",
                name="etl_html_seudinheiro_node",
                tags=["pipeline-ingestion", "noticias"],
            ),
            node(
                func=extract_transform_moneytimes,
                inputs=["params:moneytimes_parms", "parameters"],
                outputs="stage_moneytimes",
                name="etl_html_moneytimes_node",
                tags=["pipeline-ingestion", "noticias"],
            ),
        ]
    )
