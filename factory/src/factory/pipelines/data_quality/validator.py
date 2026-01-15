"""
- Validações de Schema

"""
import logging
from dataclasses import dataclass, asdict
from typing import Dict, List
import pandas as pd
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class QualityMetrics:
    """Métricas de qualidade de dados para monitoramento."""
    metrics_id: str
    dataset_name: str
    check_timestamp: str
    total_records: int
    null_count: Dict[str, int]
    duplicate_count: int
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
    """Classe para validação e qualidade de dados."""

    def __init__(self, dataset_name: str, thresholds: Dict[str, float] = None):
        self.dataset_name = dataset_name
        self.thresholds = thresholds
        self.warnings = []
        self.errors = []
    
    def validate_schema(self, df: pd.DataFrame, expected_schema: Dict[str, str]):
        """Valida o schema do DataFrame."""
        errors = []
        
        # Comparar colunas
        missing_columns = set(expected_schema.keys()) - set(df.columns)
        if missing_columns:
            errors.append(f"Colunas faltando: {missing_columns}")
        
        # Verifica tipos de dados
        for col, expected_type in expected_schema.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if not self._is_compatible_type(actual_type, expected_type):
                    errors.append(f"Coluna '{col}' tem tipo '{actual_type}' incompativel com esperado '{expected_type}'")
        
        return len(errors) == 0, errors

    def validate_date_column(self, df: pd.DataFrame, date_column: str = 'dat_ref'):
        """Valida a coluna de data."""
        errors = []

        try:
            dates = pd.to_datetime(df[date_column], errors='coerce')

            # Verifica datas invalidas
            invalid_dates = dates.isnull().sum()
            if invalid_dates > 0:
                errors.append(f"{invalid_dates} datas inválidas em '{date_column}'")

            # Verifica datas no futuro
            today = pd.Timestamp.now().normalize()
            future_dates = (dates > today).sum()
            if future_dates > 0:
                errors.append(f"{future_dates} datas no futuro em '{date_column}'")

            # Verifica dadas muito antigas
            old_dates = (dates < (today - pd.Timedelta(days=3650))).sum()
            if old_dates > 0:
                self.warnings.append(f"{old_dates} datas com mais de 10 anos em '{date_column}'")

        except Exception as e:
            errors.append(f"Erro ao validar datas em '{date_column}': {str(e)}")

        return len(errors) == 0, errors

    def validate_nulls(self, df: pd.DataFrame, nullable_cols: List[str] = None):
        """Verifica valores nulos"""
        nullable_cols = nullable_cols or []

        null_counts = {col: int(df[col].isnull().sum()) for col in df.columns}
            
        self.warnings.append(
            [
                f"Coluna '{col}' contém {count} valores nulos"
                for col, count in null_counts.items()
                if col not in nullable_cols and count > 0
            ])

        return null_counts

    def validade_duplicates(self, df: pd.DataFrame, key_columns: List[str]):
        """Verifica chaves duplicados"""
        duplicates = df.duplicated(subset=key_columns, keep='first').sum()
        
        if duplicates > 0:
            self.warnings.append(f"Encontrados {duplicates} duplicados nas colunas {key_columns}")
        
        return int(duplicates)

    def detect_outliers(self, df: pd.DataFrame, numeric_cols: List[str], method: str = 'iqr'):
        """Detecta outliers em colunas numericas"""
        outlier_counts = {}

        for col in numeric_cols:
            if col not in df.columns:
                continue

            if method == 'iqr':
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                
                IQR  = Q3 - Q1
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR

                outliers = ((df[col] < lower) | (df[col] > upper)).sum()

            elif method == 'zscore':
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                outliers = (z_scores > 3).sum()

            else:
                outliers = 0

            outlier_counts[col] = int(outliers)

            if outliers > 0:
                pct = (outliers / len(df)) * 100
                if pct > self.thresholds['outlier_max_pct']:
                    self.warnings.append(f"Coluna '{col}': {outliers} outliers ({pct:.2f}%)")
        
        return outlier_counts

    def validate_price_consistency(self, df: pd.DataFrame):
        """Valida a consistencia entre preços"""
        
        # High deve ser >= Low

        # Close deve estar entre Low e High
        
        # Open deve estar entre Low e High

    def validate_price_consistency(self, df: pd.DataFrame):
        """Valida a consistencia entre preços."""
        errors: List[str] = []

        # Coerção numérica segura (valores inválidos viram NaN)
        high = pd.to_numeric(df["high_price"], errors="coerce")
        low = pd.to_numeric(df["low_price"], errors="coerce")
        close = pd.to_numeric(df["close_price"], errors="coerce")
        open = pd.to_numeric(df["open_price"], errors="coerce")

        # denominador para percentuais (linhas válidas onde low ou high não são ambos NaN)
        total = len(df)

        # Masks de validade (tratando NaN como inválido para as comparações)
        high_ge_low_mask = (high >= low) & high.notna() & low.notna()
        close_between_mask = (close >= low) & (close <= high) & close.notna() & low.notna() & high.notna()

        n_high_low_viol = int((~high_ge_low_mask).sum())
        n_close_viol = int((~close_between_mask).sum())

        # Checar open se existir
        n_open_viol = 0
        open_between_mask = (open >= low) & (open <= high) & open.notna() & low.notna() & high.notna()
        n_open_viol = int((~open_between_mask).sum())

        # Percentuais
        pct_high_low_viol = 100.0 * n_high_low_viol / total
        pct_close_viol = 100.0 * n_close_viol / total
        pct_open_viol = 100.0 * n_open_viol / total if "open" in df.columns else 0.0

        if n_high_low_viol > 0:
            sample_vals = df.loc[~high_ge_low_mask, "dat_ref"].head(5)
            sample_list = sample_vals.astype(str).tolist()
            errors.append(
                f"{n_high_low_viol} linhas ({pct_high_low_viol:.2f}%) com high < low. Exemplos dat_ref: {sample_list}"
            )

        if n_close_viol > 0:
            sample_vals = df.loc[~close_between_mask, "dat_ref"].head(5)
            sample_list = sample_vals.astype(str).tolist()
            errors.append(
                f"{n_close_viol} linhas ({pct_close_viol:.2f}%) com close fora do intervalo [low, high]. Exemplos dat_ref: {sample_list}"
            )

        if n_open_viol > 0:
            sample_vals = df.loc[~open_between_mask, "dat_ref"].head(5)
            sample_list = sample_vals.astype(str).tolist()
            errors.append(
                f"{n_open_viol} linhas ({pct_open_viol:.2f}%) com open fora do intervalo [low, high]. Exemplos dat_ref: {sample_list}"
            )

        failed = any(
            pct > float(self.thresholds.get("consistency_min"))
            for pct in (pct_high_low_viol, pct_close_viol, pct_open_viol)
        )

        # guardar warnings se pequenas inconsistências (não bloqueantes)
        if not failed and errors:
            self.warnings.append(f"Pequenas inconsistências de preço detectadas: {errors}")

        # se falhou, também registrar como error interno
        if failed:
            self.errors.extend(errors)

        return (not failed), errors

    def calculate_quality_metrics(self, df: pd.DataFrame, null_counts: Dict[str, int],
                                  duplicate_counts: int, outlier_counts: Dict[str, int]):
        """Calcula métricas agregadas de qualidade"""
        total_records = len(df)
        total_cells = total_records * len(df.columns)
        total_nulls = sum(null_counts.values())
        total_outliers = sum(outlier_counts.values())
        
        #
        nulls_pct = ((total_cells - total_nulls) / total_cells) * 100 if total_cells > 0 else 0
        validity_pct = ((total_records - total_outliers) / total_records) * 100 if total_records > 0 else 0
        consistency_pct = ((total_records - duplicate_counts) / total_records) * 100 if total_records > 0 else 0
        
        # Score de qualidade
        quality_score = (
            nulls_pct * 0.4 +
            validity_pct * 0.3 +
            consistency_pct * 0.3
        )

        if len(self.errors) > 0:
            status = 'FAILED'
        elif len(self.warnings) > 0:
            status = 'WARNING'
        else:
            status = 'PASSED'
        
        return QualityMetrics(
            metrics_id=f'{self.dataset_name}_{datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}',
            dataset_name=self.dataset_name,
            check_timestamp=datetime.now().isoformat(),
            total_records=total_records,
            null_count=null_counts,
            duplicate_count=duplicate_counts,
            completeness_pct=round(nulls_pct, 2),
            validity_pct=round(validity_pct, 2),
            consistency_pct=round(consistency_pct, 2),
            outlier_count=total_outliers,
            quality_score=round(quality_score, 2),
            warnings=self.warnings.copy(),
            errors=self.errors.copy(),
            status=status
        )

    def _is_compatible_type(self, actual: str, expected: str) -> bool:
        type_mapping = {
            'int64': ['int64', 'Int64', 'float64'],
            'float64': ['float64'],
            'object': ['object', 'string'],
            'string': ['string', 'object'],
            'bool': ['bool']
        }
        
        compatible_types = type_mapping.get(expected.lower(), [expected.lower()])
        return actual.lower() in compatible_types or expected.lower() in actual.lower()
