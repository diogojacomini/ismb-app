"""
This is a boilerplate pipeline 'data_quality' generated using Kedro 0.19.14
"""
from datetime import datetime
import logging
import pandas as pd
from .validator import DataQualityValidator, QualityMetrics
from factory.saiph.schemas import SchemaRegistry

logger = logging.getLogger(__name__)


def validate_stage_mercado(
    df: pd.DataFrame,
    dim_tempo: pd.DataFrame,
    dataset_name: str,
    params_quality: dict,
    params_global: dict,
) -> pd.DataFrame:
    """Valida um dataset de mercado e retorna as metricas de qualidade."""
    odate = params_global.get("odate")
    process_full_data = params_global.get("process_full_data", False)
    sample_period = params_global.get("sample_period_month", {}).get(dataset_name)
    logger.info("Odate: %s", odate)
    logger.info("Quality thresholds: %s", params_quality)

    if sample_period and not process_full_data:
        logger.info("Periodo de analise: %s meses", sample_period)
        dim_tempo["dat_ref_fmt"] = pd.to_datetime(dim_tempo["dat_ref"], errors="coerce")
        start_date = pd.to_datetime(odate, errors="coerce") - pd.DateOffset(
            months=int(sample_period)
        )
        dim_tempo = dim_tempo.loc[(dim_tempo["dat_ref_fmt"] >= start_date) &
                                  (dim_tempo["dat_ref_fmt"] <= pd.to_datetime(odate, errors="coerce"))]

    # Filtra apenas dias uteis (quando é coletado tudo, vem finais de semana e feriados)
    df = dim_tempo[dim_tempo["dia_util"] == 1][["dat_ref"]].merge(
        df, on="dat_ref", how="left"
    )

    validator = DataQualityValidator(dataset_name, params_quality)
    logger.info("Validating dataset '%s' with %d records.", dataset_name, len(df))

    consistency_price = validator.validate_price_consistency(df)
    metrics = _pre_validation_metrics(validator, df, dataset_name, odate, consistency_price)

    logger.info(
        "Qualidade %s: Score=%.2f, Status=%s",
        dataset_name,
        metrics.quality_score,
        metrics.status,
    )

    if metrics.status == "FAILED":
        logger.error("Validacao FALHOU para %s: %s", dataset_name, metrics.errors)
        raise ValueError("Data Quality Check FAILED para %s" % dataset_name)

    _log_warnings(validator.warnings)
    return pd.DataFrame([metrics.to_dict()])


def validate_stage_noticias(
    df: pd.DataFrame,
    dataset_name: str,
    params_quality: dict,
    params_global: dict,
) -> pd.DataFrame:
    """Valida um dataset de noticias e retorna as metricas de qualidade."""
    odate = params_global.get("odate")
    process_full_data = params_global.get("process_full_data", False)
    logger.info("Odate: %s", odate)
    logger.info("Quality thresholds: %s", params_quality)

    if not process_full_data:
        df = df[df["dat_ref"] == odate]

    validator = DataQualityValidator(dataset_name, params_quality)
    logger.info("Validating dataset '%s' with %d records.", dataset_name, len(df))

    consistency_text = validator.consistency_text_fields(df)
    metrics = _pre_validation_metrics(validator, df, dataset_name, odate, consistency_text)

    logger.info(
        "Qualidade %s: Score=%.2f, Status=%s",
        dataset_name,
        metrics.quality_score,
        metrics.status,
    )

    if metrics.status == "FAILED":
        logger.error("Validacao FALHOU para %s: %s", dataset_name, metrics.errors)
        raise ValueError("Data Quality Check FAILED para %s" % dataset_name)

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
    params_global: dict,
) -> pd.DataFrame:
    """Consolida as metricas de todos os relatorios individuais em um unico relatorio de qualidade."""
    odate = params_global.get("odate")
    process_full_data = params_global.get("process_full_data", False)
    logger.info("Odate: %s", odate)

    df_report = pd.concat(
        [
            metrics_stage_cds,
            metrics_stage_ibov,
            metrics_stage_ivvb,
            metrics_stage_ifix,
            metrics_stage_infomoney,
            metrics_stage_valorinveste,
            metrics_stage_seudinheiro,
            metrics_stage_moneytimes,
        ],
        ignore_index=True,
    )

    if not process_full_data:
        df_report = df_report[df_report["dat_ref"] == odate]

    df_report["check_timestamp"] = pd.to_datetime(df_report["check_timestamp"])
    df_report = df_report.sort_values("check_timestamp", ascending=False)
    df_report = df_report.drop_duplicates(subset=["dataset_name", "dat_ref"], keep="first")

    df_report["execution_date"] = datetime.now().isoformat()

    logger.info("Relatorio gerado com %d linhas.", len(df_report))
    logger.info("Score medio de qualidade: %.2f", df_report["quality_score"].mean())

    return df_report


def _pre_validation_metrics(
    validator: DataQualityValidator,
    df: pd.DataFrame,
    dataset_name: str,
    dat_ref: str,
    consistency: float = None,
) -> QualityMetrics:
    """Executa as validacoes basicas e retorna o QualityMetrics calculado."""
    is_schema_valid, schema_errors = validator.validate_schema(df, SchemaRegistry.get_schema(dataset_name))
    if not is_schema_valid:
        validator.errors.extend(schema_errors)
        logger.info("schema_errors: %s", schema_errors)

    logger.info("is_schema_valid: %s", is_schema_valid)

    is_date_valid, date_errors = validator.validate_date_column(df, "dat_ref")
    if not is_date_valid:
        validator.errors.extend(date_errors)
        logger.info("date_errors: %s", date_errors)

    logger.info("is_date_valid: %s", is_date_valid)

    null_counts = validator.validate_nulls(df, [])
    if sum(null_counts.values()) > 0:
        validator.errors.extend(null_counts)
        logger.warning("Valores nulos presentes: %s", null_counts)

    numeric_cols = df.select_dtypes(include=["int64", "float64", "int32", "float32"]).columns.tolist()
    outlier_counts = validator.detect_outliers(df, numeric_cols, method="iqr")

    return validator.calculate_quality_metrics(
        df, null_counts, outlier_counts, dat_ref, consistency
    )


def _log_warnings(warnings: list) -> None:
    """Emite cada aviso acumulado como logger.warning."""
    for w in warnings:
        logger.warning(w)
