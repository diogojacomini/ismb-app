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
        logger.info(f"schema_errors: {schema_errors}")
        # logger.info(f"validator.errors: {validator.errors}")

    logger.info(f"is_schema_valid: {is_schema_valid}")

    # Validar Datas
    is_data_valid, data_errors = validator.validate_date_column(df, 'dat_ref')
    if not is_data_valid:
        validator.errors.extend(data_errors)
        logger.info(f"data_errors: {data_errors}")

    logger.info(f"is_data_valid: {is_data_valid}")

    # Valida Nulos
    nullable_cols = []
    null_counts = validator.validate_nulls(df, nullable_cols)
    logger.info(null_counts)
    
    # Valida duplicados
    duplicate_count = validator.validade_duplicates(df, ['dat_ref'])
    
    # Identificação de outliers
    numeric_cols = df.select_dtypes(include=['int64', 'float64', 'int32', 'float32']).columns.to_list()
    outlier_counts = validator.detect_outliers(df, numeric_cols, method='iqr')

    # Validações de négocio
    out_f, errossss = validator.validate_price_consistency(df)
    
    # Calculo das métricas
    metrics = validator.calculate_quality_metrics(df, null_counts, duplicate_count, outlier_counts)
    
    logger.info(f"Qualidade {dataset_name}: Score={metrics.quality_score:.2f}, Status={metrics.status}")
    
    if metrics.status == 'FAILED':
        logger.error(f"Validação FALHOU para {dataset_name}: {metrics.errors}")
        raise ValueError(f"Data Quality Check FAILED para {dataset_name}")
    
    logger.info(f"validator.warnings: {validator.warnings}")
    return pd.DataFrame([metrics.to_dict()])

def validate_stage_noticias(df: pd.DataFrame, dataset_name: str, params_quality: dict, params_global: dict) -> pd.DataFrame:
    logger.info(f"Validating dataset STAGE: {dataset_name} with {len(df)} records.")

    return df

def generate_quality_report(
    metrics_stage_cds,
    metrics_stage_ibov,
    metrics_stage_ivvb,
    metrics_stage_ifix,
    metrics_stage_infomoney,
    metrics_stage_valorinveste,
    metrics_stage_seudinheiro,
    metrics_stage_moneytimes,
    params_global,
    # metrics_consolidated_noticias,
    # metrics_consolidated_mercado
    ) -> pd.DataFrame:
    return pd.DataFrame()

def validate_data_consolidated(df: pd.DataFrame, dataset_name: str, params_global: dict) -> pd.DataFrame:
    return pd.DataFrame()
