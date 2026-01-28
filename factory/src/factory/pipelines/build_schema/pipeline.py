"""
This is a boilerplate pipeline 'build_schema'
generated using Kedro 0.19.14
Construção do Data Warehouse em arquitetura Star Schema.

Camadas:
- Dimenções: dim_*
- Fatos: fato_*
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import (
    build_dim_tempo,
    build_dim_indice,
    build_fonte_noticia,
    build_dim_indicador,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            # Dimenções
            node(
                func=build_dim_tempo,
                inputs=["parameters"],
                outputs="dim_tempo",
                name="build_dim_tempo_node",
                tags=["pipeline-star_schema_build", "dimensoes"],
            ),
            
            node(
                func=build_dim_indice,
                inputs=["parameters"],
                outputs="dim_indice",
                name="build_dim_indice_node",
                tags=["pipeline-star_schema_build", "dimensoes"],
            ),
            
            node(
                func=build_fonte_noticia,
                inputs=["parameters"],
                outputs="dim_fonte_noticia",
                name="build_dim_fonte_noticia_node",
                tags=["pipeline-star_schema_build", "dimensoes"],
            ),
            
            node(
                func=build_dim_indicador,
                inputs=["parameters"],
                outputs="dim_indicador",
                name="build_dim_indicador_node",
                tags=["pipeline-star_schema_build", "dimensoes"],
            ),

        ]
    )
