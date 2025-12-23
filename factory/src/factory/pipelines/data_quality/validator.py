"""
- Validações de Schema

"""
import logging
from dataclasses import dataclass, asdict
from typing import Dict, List
import pandas as pd


logger = logging.getLogger(__name__)

@dataclass
class QualityMetrics:
    """Métricas de qualidade de dados para monitoramento."""
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
        self.tresholds = thresholds
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

    def _is_compatible_type(self, actual: str, expected: str) -> bool:
        type_mapping = {
            'int64': ['int64', 'Int64', 'float64'],
            'float64': ['float64'],
            'object': ['object', 'string'],
            'string': ['string', 'object'],
            'bool': ['bool']
        }
        
        compatible_types = type_mapping.get(expected, [expected])
        return actual in compatible_types or expected in actual
