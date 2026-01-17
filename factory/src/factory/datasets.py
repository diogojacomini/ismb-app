"""
Datasets customizados para particionamento automático por odate e append em CSV
"""
import logging
from typing import Any, Dict, List, Optional
from kedro.io import AbstractDataset
import pandas as pd
import fsspec
from .saiph.schemas import SchemaRegistry

logger = logging.getLogger(__name__)


class AppendCSVDataset(AbstractDataset):
    """
    Dataset que faz append e remove duplicatas por dat_ref, mantendo o mais recente
    """

    def __init__(self, filepath: str, load_args: Optional[Dict[str, Any]] = None, save_args: Optional[Dict[str, Any]] = None,
                 credentials: Optional[Dict[str, Any]] = None, fs_args: Optional[Dict[str, Any]] = None, environment: str = 'prd'):
        self._filepath: str = filepath
        self._load_args: Dict[str, Any] = load_args or {}
        self._save_args: Dict[str, Any] = save_args or {}
        self.environment = environment

        self._storage_options: Dict[str, Any] = {}
        if credentials:
            self._storage_options.update(credentials)
        
        if fs_args:
            self._storage_options.update(fs_args)
        
        if self.environment == 'dev' or self.environment == 'test':
            self._filepath = self._filepath.replace('data/', 'data/sandbox/dev/')

    def _load(self) -> pd.DataFrame:
        if self._exists():
            load_kwargs = dict(self._load_args)
            load_kwargs.setdefault('storage_options', self._storage_options)
            return pd.read_csv(self._filepath, **load_kwargs)

        return pd.DataFrame()

    def _save(self, data):
        existing: pd.DataFrame = self._load()

        data = SchemaRegistry._apply_schema(data, name=self._filepath.split('/')[-1].split('.')[0])
        combined: pd.DataFrame = pd.concat([existing, data], ignore_index=True)

        if 'sk' in combined.columns[0]:
            keys_order_subset: List[str] = [combined.columns[0]]
            combined = combined.drop_duplicates(subset=keys_order_subset, keep='first')

        elif 'fonte' in combined.columns:
            keys_order_subset: List[str] = ['dat_ref', 'id_news', 'fonte']
            combined = combined.sort_values(keys_order_subset, ascending=False)
            combined = combined.drop_duplicates(subset=keys_order_subset, keep='last')

        elif 'metrics' in combined.columns:
            combined = combined.sort_values('start_time', ascending=False)

        elif 'metrics_id' in combined.columns:
            combined = combined.sort_values('check_timestamp', ascending=False)

        elif ('dat_ref' in combined.columns) and ('cod_indice' in combined.columns):
            combined = combined.sort_values(['dat_ref', 'cod_indice'], ascending=False)
            combined = combined.drop_duplicates(subset=['dat_ref', 'cod_indice'], keep='last')

        elif ('dat_ref' in combined.columns) and ('cod_fonte' in combined.columns):
            combined = combined.sort_values(['dat_ref', 'cod_fonte'], ascending=False)
            combined = combined.drop_duplicates(subset=['dat_ref', 'cod_fonte'], keep='last')

        else:
            combined = combined.sort_values('dat_ref', ascending=False)
            combined = combined.drop_duplicates(subset=['dat_ref'], keep='last')
            if len(data) == 0:
                raise ValueError("Dataset vazio!")

            logger.info('dataset to save:')
            logger.info(data)

        save_kwargs = dict(self._save_args)
        save_kwargs.setdefault('storage_options', self._storage_options)
        if self.environment == 'hk':
            self._filepath = self._filepath.replace('data/', 'data/sandbox/')

        combined.to_csv(self._filepath, index=False, **save_kwargs)

    def _exists(self) -> bool:
        storage_options: Dict[str, Any] = self._load_args.get('storage_options', {}) or self._storage_options or {}
        fs, path = fsspec.core.url_to_fs(self._filepath, **storage_options)
        try:
            exist_file = fs.exists(path)
        except Exception as error_file_empty:
            exist_file = False
            logger.info("Arquivo não existe: %s", error_file_empty)

        return exist_file

    def _describe(self) -> Dict[str, Any]:
        return {'filepath': self._filepath}
