"""
This is a boilerplate pipeline 'data_quality'
generated using Kedro 0.19.14

"""
import pandas as pd
import logging
from .validator import DataQualityValidator
from factory.saiph.schemas import SchemaRegistry
from datetime import datetime

logger = logging.getLogger(__name__)


def validate_stage_mercado(df: pd.DataFrame, dataset_name: str, params_quality: dict, params_global: dict) -> pd.DataFrame:
    
    # Parametros
    odate = params_global.get("odate")
    process_full_data = params_global.get("process_full_data", False)
    sample_period = params_global.get("sample_period_month").get(dataset_name, None)
    logger.info(f"Odate de processamento: {odate}")
    logger.info(f"Quality thresholds: {params_quality}")

    if sample_period and not process_full_data:
        logger.info(f"Periodo de Analise: {sample_period}")

        df['dat_ref_fmt'] = pd.to_datetime(df['dat_ref'], errors="coerce")
        _start_date = pd.to_datetime(odate, errors="coerce") - pd.DateOffset(months=int(sample_period))
        df = df.loc[(df['dat_ref_fmt'] >= _start_date) & (df['dat_ref_fmt'] <= pd.to_datetime(odate, errors="coerce"))].drop(columns=['dat_ref_fmt'])

    validator = DataQualityValidator(dataset_name, params_quality)
    logger.info(f"Validating dataset: {dataset_name} with {len(df)} records.")

    # Pre validações
    metrics = _pre_validation_metrics(validator, df, dataset_name, odate)

    # Validações de négocio, apenas warnings
    validator.validate_price_consistency(df)

    logger.info(f"Qualidade {dataset_name}: Score={metrics.quality_score:.2f}, Status={metrics.status}")

    if metrics.status == 'FAILED':
        logger.error(f"Validação FALHOU para {dataset_name}: {metrics.errors}")
        raise ValueError(f"Data Quality Check FAILED para {dataset_name}")

    _log_warnings(validator.warnings)
    return pd.DataFrame([metrics.to_dict()])

def validate_stage_noticias(df: pd.DataFrame, dataset_name: str, params_quality: dict, params_global: dict) -> pd.DataFrame:
    
    # Parametros
    odate = params_global.get("odate")
    process_full_data = params_global.get("process_full_data", False)
    logger.info(f"Odate de processamento: {odate}")
    logger.info(f"Quality thresholds: {params_quality}")

    if not process_full_data:
        df = df[df["dat_ref"] == odate]

    validator = DataQualityValidator(dataset_name, params_quality)
    logger.info(f"Validating dataset: {dataset_name} with {len(df)} records.")
    
    # Pre validações
    metrics = _pre_validation_metrics(validator, df, dataset_name, odate)
    
    # Validações de négocio, apenas warnings
    validator.validate_text_fields(df, ['titulo', 'link'])

    logger.info(f"Qualidade {dataset_name}: Score={metrics.quality_score:.2f}, Status={metrics.status}")
    
    if metrics.status == 'FAILED':
        logger.error(f"Validação FALHOU para {dataset_name}: {metrics.errors}")
        raise ValueError(f"Data Quality Check FAILED para {dataset_name}")

    _log_warnings(validator.warnings)
    return pd.DataFrame([metrics.to_dict()])

def generate_quality_report(
    metrics_stage_cds: pd.DataFrame,
    metrics_stage_ibov: pd.DataFrame,
    metrics_stage_ivvb: pd.DataFrame,
    metrics_stage_ifix: pd.DataFrame,
    metrics_stage_infomoney: pd.DataFrame,
    metrics_stage_valorinveste: pd.DataFrame,
    metrics_stage_seudinheiro: pd.DataFrame,
    metrics_stage_moneytimes: pd.DataFrame,
    params_global,
    ) -> pd.DataFrame:
    
    # Parametros
    odate = params_global.get("odate")
    process_full_data = params_global.get("process_full_data", False)
    logger.info(f"Odate de processamento: {odate}")

    df_report = pd.concat([
        metrics_stage_cds,
        metrics_stage_ibov,
        metrics_stage_ivvb,
        metrics_stage_ifix,
        metrics_stage_infomoney,
        metrics_stage_valorinveste,
        metrics_stage_seudinheiro,
        metrics_stage_moneytimes,
    ], ignore_index=True)

    if not process_full_data:
        df_report = df_report[df_report["dat_ref"] == odate]

    # Contagem da quantidade de erros e do score: warnings,errors
    df_report["warning_count"] = (
        df_report["warnings"]
        .astype(str)
        .str.replace(r"[\[\]\s]", "", regex=True)
        .ne("")
        .astype(int)
    )

    df_report["error_count"] = (
        df_report["errors"]
        .astype(str)
        .str.replace(r"[\[\]\s]", "", regex=True)
        .ne("")
        .astype(int)
    )

    df_report['execution_date'] = datetime.now().strftime('%Y-%m-%d')
    df_report['execution_date'] = datetime.now().isoformat()

    logger.info(f"Relatório gerado com {len(df_report)} linhas.")
    logger.info(f"Score médio de qulidade: {df_report['quality_score'].mean():.2f}")

    return df_report

def _pre_validation_metrics(validator, df, dataset_name, dat_ref):
    # Validação de schema e tipo de dados
    is_schema_valid, schema_errors = validator.validate_schema(df, SchemaRegistry.get_schema(dataset_name))
    if not is_schema_valid:
        validator.errors.extend(schema_errors)
        logger.info(f"schema_errors: {schema_errors}")

    logger.info(f"is_schema_valid: {is_schema_valid}")

    # Validar Datas
    is_date_valid, data_errors = validator.validate_date_column(df, 'dat_ref')
    if not is_date_valid:
        validator.errors.extend(data_errors)
        logger.info(f"data_errors: {data_errors}")

    logger.info(f"is_date_valid: {is_date_valid}")

    # Valida Nulos
    nullable_cols = []
    null_counts = validator.validate_nulls(df, nullable_cols)
    if sum(null_counts.values()) > 0:
        logger.warning(f"Valores Nulls presente: {null_counts}")

    # Valida duplicados
    duplicate_count = validator.validade_duplicates(df, ['dat_ref'])

    # Identificação de outliers
    numeric_cols = df.select_dtypes(include=['int64', 'float64', 'int32', 'float32']).columns.to_list()
    outlier_counts = validator.detect_outliers(df, numeric_cols, method='iqr')

    # Calculo das métricas
    return validator.calculate_quality_metrics(df, null_counts, duplicate_count, outlier_counts, dat_ref)

def _log_warnings(warnings):
    for w in warnings:
        logger.warning(w)
