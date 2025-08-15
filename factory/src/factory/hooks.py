"""
Hooks personalizados para particionamento por data
"""
import logging
from typing import Any
from kedro.framework.hooks import hook_impl
import pandas as pd

logger = logging.getLogger(__name__)


class DataPartitioningHook:
    """Hook para salvar dados particionados por odate"""

    @hook_impl
    def before_dataset_saved(self, dataset_name: str, data: Any, node: Any = None) -> None:
        """
        Modifica o caminho do arquivo para incluir partição por odate
        """
        # Verifica se o dataset tem particionamento habilitado
        if (hasattr(data, 'columns') and 'odate' in data.columns and isinstance(data, pd.DataFrame) and not data.empty):

            # Pega a data da primeira linha (assumindo que todas as linhas têm a mesma data)
            odate = str(data['odate'].iloc[0])

            logger.info("Particionamento detectado: %s para data %s", dataset_name, odate)

            # Remove a coluna odate do DataFrame antes de salvar
            if 'odate' in data.columns:
                data.drop(columns=['odate'], inplace=True)
                logger.info("Coluna 'odate' removida do DataFrame %s", dataset_name)
