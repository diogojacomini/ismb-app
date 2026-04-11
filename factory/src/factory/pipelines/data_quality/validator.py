"""Validacoes de schema, dados e qualidade para datasets do pipeline.

O DataQualityValidator realiza verificacoes de schema, datas, nulos,
duplicados, outliers e consistencia de precos e noticias. 
No final compila as metricas e determina o status: PASSED, WARNING ou FAILED.
"""
import logging
from dataclasses import dataclass, asdict
from typing import Dict, List
import pandas as pd
import numpy as np
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)


@dataclass
class QualityMetrics:
    """Métricas de qualidade de dados para monitoramento."""

    metrics_id: str
    dataset_name: str
    dat_ref: str
    check_timestamp: str
    total_records: int
    null_count: Dict[str, int]
    completeness_pct: float
    validity_pct: float
    consistency_pct: float
    outlier_count: int
    quality_score: float
    warnings: List[str]
    errors: List[str]
    status: str

    def to_dict(self) -> Dict[str, any]:
        return asdict(self)


class DataQualityValidator:
    """Executa validacoes de qualidade em um DataFrame e acumula avisos e erros."""

    def __init__(self, dataset_name: str, thresholds: Dict[str, float] | None = None) -> None:
        self.dataset_name = dataset_name
        self.thresholds: Dict[str, float] = thresholds or {}
        self.warnings: List[str] = []
        self.errors: List[str] = []

    def validate_schema(self, df: pd.DataFrame, expected_schema: Dict[str, str]) -> tuple[bool, List[str]]:
        """Verifica se o DataFrame possui as colunas esperadas e os tipos compativeis."""
        errors: List[str] = []

        missing_columns = set(expected_schema.keys()) - set(df.columns)
        if missing_columns:
            errors.append("Colunas faltando: %s" % missing_columns)

        for col, expected_type in expected_schema.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if not self._is_compatible_type(actual_type, expected_type):
                    errors.append("Coluna '%s' tem tipo '%s', esperado '%s'" % (col, actual_type, expected_type))

        return len(errors) == 0, errors

    def validate_date_column(self, df: pd.DataFrame, date_column: str = "dat_ref") -> tuple[bool, List[str]]:
        """Valida a coluna de data: datas invalidas, futuras e muito antigas."""
        errors: List[str] = []

        try:
            dates = pd.to_datetime(df[date_column], errors="coerce")

            invalid_dates = dates.isnull().sum()
            if invalid_dates > 0:
                errors.append("%d datas invalidas em '%s'" % (invalid_dates, date_column))

            today = pd.Timestamp.now().normalize()
            future_dates = (dates > today).sum()
            if future_dates > 0:
                errors.append("%d datas no futuro em '%s'" % (future_dates, date_column))

            old_dates = (dates < (today - pd.Timedelta(days=3650))).sum()
            if old_dates > 0:
                self.warnings.append("%d datas com mais de 10 anos em '%s'" % (old_dates, date_column))

        except Exception as e:
            errors.append("Erro ao validar datas em '%s': %s" % (date_column, str(e)))

        return len(errors) == 0, errors

    def validate_nulls(self, df: pd.DataFrame, nullable_cols: List[str] | None = None) -> Dict[str, int]:
        """Conta valores nulos por coluna e registra avisos para colunas nao-nulificaveis."""
        nullable_cols = nullable_cols or []

        null_counts: Dict[str, int] = {
            col: int(df[col].isnull().sum()) for col in df.columns
        }

        self.warnings.extend(
            "Coluna '%s' contem %d valores nulos" % (col, count)
            for col, count in null_counts.items()
            if col not in nullable_cols and count > 0
        )

        return null_counts

    def validate_duplicates(self, df: pd.DataFrame, key_columns: List[str] | str) -> int:
        """Conta duplicatas pelas colunas chave e registra aviso se houver."""
        if isinstance(key_columns, str):
            key_columns = [key_columns]

        duplicates = df.duplicated(subset=key_columns, keep="first").sum()

        if duplicates > 0:
            self.warnings.append("Encontrados %d duplicados nas colunas %s" % (duplicates, key_columns))

        return int(duplicates)

    def detect_outliers(
        self,
        df: pd.DataFrame,
        numeric_cols: List[str],
        method: str = "iqr",
    ) -> Dict[str, int]:
        """Detecta outliers nas colunas numericas usando IQR ou Z-score."""
        outlier_counts: Dict[str, int] = {}

        for col in numeric_cols:
            if col not in df.columns:
                continue

            if method == "iqr":
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                outliers = ((df[col] < lower) | (df[col] > upper)).sum()

            elif method == "zscore":
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                outliers = (z_scores > 3).sum()

            else:
                outliers = 0

            outlier_counts[col] = int(outliers)

            if outliers > 0:
                pct = (outliers / len(df)) * 100
                max_pct = self.thresholds.get("outlier_max_pct", 100.0)
                if pct > max_pct:
                    self.warnings.append("Coluna '%s': %d outliers (%.2f%%)" % (col, outliers, pct))

        return outlier_counts

    def validate_price_consistency(self, df: pd.DataFrame) -> float:
        """Calcula um score de consistencia de preços"""
        scores = []

        # validação de ordem lógica de preços
        # high >= max(open, close) e low <= min(open, close)
        valid_high = (df["high_price"] >= df[["open_price", "close_price"]].max(axis=1)).mean()
        valid_low = (df["low_price"] <= df[["open_price", "close_price"]].min(axis=1)).mean()
        price_order_score = ((valid_high + valid_low) / 2) * 25
        scores.append(price_order_score)

        # consistencia entre open e close do dia anterior
        # o close de um dia deveria ser próximo ao open do dia seguinte
        df_sorted = df.sort_values("dat_ref")
        if len(df_sorted) > 1:
            close_open_diff = abs(df_sorted["close_price"].shift(1) - df_sorted["open_price"]).dropna()
            avg_price = df_sorted["close_price"].mean()
            relative_diff = (close_open_diff / avg_price).mean()
            close_open_score = (max(0, (1 - min(relative_diff * 10, 1))) * 20)  # quanto menor a diferença, melhor
            scores.append(close_open_score)

        # volatilidade coerente com mudança percentual
        if "change_percentage" in df.columns:
            expected_change = ((df["close_price"] - df["open_price"]) / df["open_price"] * 100)
            change_diff = abs(df["change_percentage"] - expected_change)
            change_consistency = (change_diff <= 0.5).mean()  # tolerancia de 5%
            change_score = change_consistency * 20
            scores.append(change_score)

        if "volume" in df.columns:
            volume_score = 0

            non_negative_vol = (df["volume"] >= 0).mean()
            volume_score += non_negative_vol * 8

            if "change_percentage" in df.columns or len(df_sorted) > 1:
                price_changed = df["close_price"] != df["open_price"]
                volume_on_change = (df.loc[price_changed, "volume"] > 0).sum() / max(
                    price_changed.sum(), 1
                )
                volume_score += volume_on_change * 7

            if df["volume"].std() > 0:
                vol_mean = df["volume"].mean()
                vol_std = df["volume"].std()
                vol_outliers = abs(df["volume"] - vol_mean) > (4 * vol_std)
                volume_score += max(0, (1 - vol_outliers.mean())) * 5
            else:
                volume_score += 5

            scores.append(volume_score)

        # high - low deve ser razoável
        price_range = df["high_price"] - df["low_price"]
        non_zero_range = (price_range > 0).mean()
        range_score = non_zero_range * 15
        scores.append(range_score)

        completeness = (1 - df.isnull().mean().mean()) * 10
        scores.append(completeness)

        # Score final
        total_score = sum(scores)

        return round(min(100, max(0, total_score)), 2)

    def calculate_quality_metrics(
        self,
        df: pd.DataFrame,
        null_counts: Dict[str, int],
        outlier_counts: Dict[str, int],
        dat_ref: str,
        consistency: float = None,
    ) -> "QualityMetrics":
        """Calcula metricas agregadas de qualidade e retorna um QualityMetrics."""
        total_records = len(df)
        total_cells = total_records * len(df.columns)
        total_nulls = sum(null_counts.values())
        total_outliers = sum(outlier_counts.values())

        completeness_pct = (((total_cells - total_nulls) / total_cells) * 100 if total_cells > 0 else 0)
        validity_pct = (((total_records - total_outliers) / total_records) * 100 if total_records > 0 else 0)
        consistency_pct = consistency if consistency is not None else 100.0

        quality_score = (completeness_pct * 0.75 + validity_pct * 0.05 + consistency_pct * 0.20)

        if self.errors:
            status = "FAILED"
        elif self.warnings:
            status = "WARNING"
        else:
            status = "PASSED"

        metrics_id = "%s_%s" % (self.dataset_name, uuid.uuid4().hex[:8])

        return QualityMetrics(
            metrics_id=metrics_id,
            dataset_name=self.dataset_name,
            dat_ref=dat_ref,
            check_timestamp=datetime.now().isoformat(),
            total_records=total_records,
            null_count=total_nulls,
            completeness_pct=round(completeness_pct, 2),
            validity_pct=round(validity_pct, 2),
            consistency_pct=round(consistency_pct, 2),
            outlier_count=total_outliers,
            quality_score=round(quality_score, 2),
            warnings=self.warnings.copy(),
            errors=self.errors.copy(),
            status=status,
        )

    def _is_compatible_type(self, actual: str, expected: str) -> bool:
        """Verifica se o tipo real e compativel com o tipo esperado."""
        type_mapping: Dict[str, List[str]] = {
            "int64": ["int64", "Int64", "float64"],
            "float64": ["float64"],
            "object": ["object", "string"],
            "string": ["string", "object"],
            "bool": ["bool"],
        }
        compatible = type_mapping.get(expected.lower(), [expected.lower()])
        return actual.lower() in compatible or expected.lower() in actual.lower()

    def consistency_text_fields(self, df: pd.DataFrame) -> float:
        """Calcula um score de qualidade dos titulos de notícias."""
        scores = []
        if df.empty:
            self.warnings.append("DataFrame vazio para validação de texto")
            return 0.0

        # completude de campos de titulo
        empty_count = (df["titulo"].astype(str).str.strip() == "").sum()
        completeness = ((len(df) - empty_count) / len(df)) * 100

        if empty_count > 0:
            self.warnings.append("Contem %d strings vazias" % (empty_count))

        scores.append((completeness / 100) * 40)

        # qualidade de titulos
        titulo_scores = []

        titulo_len = df["titulo"].astype(str).str.strip().str.len()
        adequate_length = ((titulo_len >= 30) & (titulo_len <= 150)).mean()  # entre 30 e 150 caracteres
        titulo_scores.append(adequate_length * 30)

        short_titles = (titulo_len < 10).sum()
        if short_titles > 0:
            self.warnings.append("%d titulos com menos de 10 caracteres" % short_titles)

        special_chars = (df["titulo"].astype(str).str.count(r"[!@#$%^&*(){}\[\]|\\/<>~`]"))
        no_spam = (special_chars <= 3).mean()
        titulo_scores.append(no_spam * 10)

        uniqueness = df["titulo"].nunique() / len(df)
        titulo_scores.append(uniqueness * 10)

        scores.extend(titulo_scores)

        # qualidade de links
        link_scores = []

        # formato válido
        valid_links = (df["link"].astype(str).str.match(r"^https?://[^\s]+$", na=False).mean())
        link_scores.append(valid_links * 5)

        # links unicos
        unique_links = df["link"].nunique() / len(df)
        link_scores.append(unique_links * 5)

        scores.extend(link_scores)

        return round(min(100, max(0, sum(scores))), 2)
