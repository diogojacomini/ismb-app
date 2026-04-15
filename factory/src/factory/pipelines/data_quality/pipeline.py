"""
This is a boilerplate pipeline 'data_quality'
generated using Kedro 0.19.14

Executa validacoes de qualidade em cada dataset de stage antes do processamento,
garantindo que os dados atendem aos criterios minimos de qualidade.

Validacoes:
    Mercado:
        - Consistencia de schema (colunas esperadas e tipos)
        - Validacao de datas (formato e range valido)
        - Deteccão de valores nulos
        - Deteccão de duplicatas por chave primaria (dat_ref)
        - Deteccão de outliers
        - Variações de precos

    Para dados de noticias:
        - Consistencia de schema
        - Validacao de datas
        - Deteccão de valores nulos
        - Deteccão de duplicatas por id_news

"""
from kedro.pipeline import node, Pipeline, pipeline
from .nodes import (
    validate_stage_mercado,
    validate_stage_noticias,
    generate_quality_report,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=validate_stage_mercado,
                inputs=[
                    "stage_cds",
                    "dim_tempo",
                    "params:dataset_name_cds",
                    "params:quality_thresholds",
                    "parameters",
                ],
                outputs="metrics_stage_cds",
                name="validate_stage_cds_node",
                tags=["pipeline-data_quality", "cds"],
            ),
            node(
                func=validate_stage_mercado,
                inputs=[
                    "stage_ibov",
                    "dim_tempo",
                    "params:dataset_name_ibov",
                    "params:quality_thresholds",
                    "parameters",
                ],
                outputs="metrics_stage_ibov",
                name="validate_stage_ibov_node",
                tags=["pipeline-data_quality", "ibov"],
            ),
            node(
                func=validate_stage_mercado,
                inputs=[
                    "stage_ivvb",
                    "dim_tempo",
                    "params:dataset_name_ivvb",
                    "params:quality_thresholds",
                    "parameters",
                ],
                outputs="metrics_stage_ivvb",
                name="validate_stage_ivvb_node",
                tags=["pipeline-data_quality", "ivvb"],
            ),
            node(
                func=validate_stage_mercado,
                inputs=[
                    "stage_ifix",
                    "dim_tempo",
                    "params:dataset_name_ifix",
                    "params:quality_thresholds",
                    "parameters",
                ],
                outputs="metrics_stage_ifix",
                name="validate_stage_ifix_node",
                tags=["pipeline-data_quality", "ifix"],
            ),
            node(
                func=validate_stage_noticias,
                inputs=[
                    "stage_infomoney",
                    "params:dataset_name_infomoney",
                    "params:quality_thresholds",
                    "parameters",
                ],
                outputs="metrics_stage_infomoney",
                name="validate_stage_infomoney_node",
                tags=["pipeline-data_quality", "noticias"],
            ),
            node(
                func=validate_stage_noticias,
                inputs=[
                    "stage_valorinveste",
                    "params:dataset_name_valorinveste",
                    "params:quality_thresholds",
                    "parameters",
                ],
                outputs="metrics_stage_valorinveste",
                name="validate_stage_valorinveste_node",
                tags=["pipeline-data_quality", "noticias"],
            ),
            node(
                func=validate_stage_noticias,
                inputs=[
                    "stage_seudinheiro",
                    "params:dataset_name_seudinheiro",
                    "params:quality_thresholds",
                    "parameters",
                ],
                outputs="metrics_stage_seudinheiro",
                name="validate_stage_seudinheiro_node",
                tags=["pipeline-data_quality", "noticias"],
            ),
            node(
                func=validate_stage_noticias,
                inputs=[
                    "stage_moneytimes",
                    "params:dataset_name_moneytimes",
                    "params:quality_thresholds",
                    "parameters",
                ],
                outputs="metrics_stage_moneytimes",
                name="validate_stage_moneytimes_node",
                tags=["pipeline-data_quality", "noticias"],
            ),
            # Relatorio final
            node(
                func=generate_quality_report,
                inputs=[
                    "metrics_stage_cds",
                    "metrics_stage_ibov",
                    "metrics_stage_ivvb",
                    "metrics_stage_ifix",
                    "metrics_stage_infomoney",
                    "metrics_stage_valorinveste",
                    "metrics_stage_seudinheiro",
                    "metrics_stage_moneytimes",
                    "parameters",
                ],
                outputs="data_quality_report",
                name="generate_quality_report_node",
                tags=["pipeline-data_quality"],
            ),
        ]
    )
