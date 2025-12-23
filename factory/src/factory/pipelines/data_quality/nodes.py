"""
This is a boilerplate pipeline 'data_quality'
generated using Kedro 0.19.14

"""
import pandas as pd
import logging
from .validator import DataQualityValidator
from factory.saiph.schemas import SchemaRegistry

logger = logging.getLogger(__name__)


def validate_stage_mercado(df: pd.DataFrame, dataset_name: str, params_quality: dict, params_global: dict) -> pd.DataFrame:
    logger.info(f"Validating dataset STAGE: {dataset_name} with {len(df)} records.")

    validator = DataQualityValidator(dataset_name, params_quality)
    
    odate = params_global.get("odate")
    logger.info(f"Odate de processamento: {odate}")
    logger.info(f"Quality thresholds: {params_quality}")

    # Validação de schema e tipo de dados
    is_schema_valid, schema_errors = validator.validate_schema(df, SchemaRegistry.get_schema(dataset_name))
    if not is_schema_valid:
        validator.errors.extend(schema_errors)
        logger.info(f"validator.errors: {validator.errors}")

    logger.info(f"is_schema_valid: {is_schema_valid}")

    return df
