"""
Datasets customizados para particionamento automático por odate e append em CSV
"""
import logging
from pathlib import Path
from typing import Any, Dict, List
from kedro.io import AbstractDataset
import pandas as pd

logger = logging.getLogger(__name__)


class AppendCSVDataset(AbstractDataset):
    """
    Dataset que faz append e remove duplicatas por dat_ref, mantendo o mais recente
    """

    def __init__(self, filepath: str, load_args: Dict[str, Any] = None, save_args: Dict[str, Any] = None):
        self._filepath: str = filepath
        self._load_args: Dict[str, Any] = load_args or {}
        self._save_args: Dict[str, Any] = save_args or {}

    def _load(self) -> pd.DataFrame:
        if Path(self._filepath).exists():
            return pd.read_csv(self._filepath, **self._load_args)

        return pd.DataFrame()

    def _save(self, data):
        existing: pd.DataFrame = self._load()
        combined: pd.DataFrame = pd.concat([existing, data], ignore_index=True)

        if 'fonte' not in combined.columns:
            combined = combined.sort_values('dat_ref', ascending=False)
            combined = combined.drop_duplicates(subset=['dat_ref'], keep='last')
        else:
            keys_order_subset: List[str] = ['dat_ref', 'fonte', 'titulo']
            combined = combined.sort_values(keys_order_subset, ascending=False)
            combined = combined.drop_duplicates(subset=keys_order_subset, keep='last')

        combined.to_csv(self._filepath, index=False, **self._save_args)

    def _exists(self) -> bool:
        return Path(self._filepath).exists()

    def _describe(self) -> Dict[str, Any]:
        return {'filepath': self._filepath}
