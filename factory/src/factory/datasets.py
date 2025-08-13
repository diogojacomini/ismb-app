"""
Dataset customizado para particionamento automático por odate
"""
import logging
from pathlib import Path
from typing import Any, Dict
from kedro.io import AbstractDataset
import pandas as pd


logger = logging.getLogger(__name__)


class PartitionedParquetDataset(AbstractDataset):
    """Dataset que salva automaticamente com partição por odate"""
    
    def __init__(
        self, 
        filepath: str, 
        save_args: Dict[str, Any] = None,
        load_args: Dict[str, Any] = None
    ):
        self._filepath = filepath
        self._save_args = save_args or {}
        self._load_args = load_args or {}
    
    def _load(self) -> pd.DataFrame:
        """Carrega dados - implementação básica"""
        try:
            return pd.read_parquet(self._filepath, **self._load_args)
        except FileNotFoundError:
            logger.warning(f"Arquivo não encontrado: {self._filepath}")
            return pd.DataFrame()
    
    def _save(self, data: pd.DataFrame) -> None:
        """Salva dados com particionamento automático por odate"""
        if data.empty:
            logger.warning("DataFrame vazio, não salvando")
            return
        
        # Verifica se tem coluna odate
        if 'odate' in data.columns:
            # Pega a data (assumindo que todas as linhas têm a mesma data)
            odate = str(data['odate'].iloc[0])
            
            # Modifica o caminho para incluir partição
            original_path = Path(self._filepath)
            parent_dir = original_path.parent
            filename = original_path.name
            
            # Cria caminho particionado
            partitioned_path = parent_dir / f"odate={odate}" / filename
            
            # Cria diretório se não existe
            partitioned_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Remove coluna odate antes de salvar
            data_to_save = data.drop(columns=['odate'])
            
            logger.info(f"Salvando com partição: {partitioned_path}")
            data_to_save.to_parquet(partitioned_path, **self._save_args)
        else:
            # Salva normalmente se não tem odate
            Path(self._filepath).parent.mkdir(parents=True, exist_ok=True)
            data.to_parquet(self._filepath, **self._save_args)
    
    def _exists(self) -> bool:
        """Verifica se o dataset existe"""
        return Path(self._filepath).exists()
    
    def _describe(self) -> Dict[str, Any]:
        """Descreve o dataset"""
        return {
            "filepath": self._filepath,
            "save_args": self._save_args,
            "load_args": self._load_args
        }
