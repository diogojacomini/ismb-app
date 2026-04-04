"""
Datasets customizados para particionamento automático por odate e append em CSV/SQL.

AppendCSVDataset  — grava em arquivo CSV com deduplicação por chave natural.
AppendSQLDataset  — grava em tabela PostgreSQL com upsert (prd) ou
                    truncate+insert (sandbox/dev/test/hk), replicando a mesma
                    lógica de deduplicação do AppendCSVDataset.
"""
import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

import fsspec
import pandas as pd
from kedro.io import AbstractDataset
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

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
            self._filepath = self._filepath.replace('/data/', '/data/sandbox/dev/')

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
            keys_order_subset: List[str] = ['dat_ref', 'titulo', 'fonte']
            combined = combined.sort_values(keys_order_subset, ascending=False)
            combined = combined.drop_duplicates(subset=keys_order_subset, keep='last')

        elif 'cod_fonte' in combined.columns:
            keys_order_subset: List[str] = ['dat_ref', 'cod_fonte', 'txt_titulo']
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

        elif ('dat_ref' in combined.columns) and ('indicator' in combined.columns):
            combined = combined.sort_values(['dat_ref', 'indicator'], ascending=False)
            combined = combined.drop_duplicates(subset=['dat_ref', 'indicator'], keep='last')

        elif 'idx_a' in combined.columns and 'idx_b' in combined.columns:
            combined = combined.sort_values(['dat_ref', 'idx_a', 'idx_b'], ascending=False)
            combined = combined.drop_duplicates(subset=['dat_ref', 'idx_a', 'idx_b'], keep='last')
        
        elif 'entity_type' in combined.columns:
            combined = combined.sort_values(['entity_type', 'entity_name', 'year'], ascending=False)
            combined = combined.drop_duplicates(subset=['entity_type', 'entity_name', 'year'], keep='last')
        
        else:
            combined = combined.drop_duplicates(subset=['dat_ref'], keep='last')
            combined = combined.sort_values('dat_ref', ascending=False)
            if len(data) == 0:
                raise ValueError("Dataset vazio!")

            logger.info('dataset to save:')
            logger.info(data)

        save_kwargs = dict(self._save_args)
        save_kwargs.setdefault('storage_options', self._storage_options)

        _filepath = self._filepath.replace('/data/', '/data/sandbox/') if self.environment == 'hk' else self._filepath

        combined.to_csv(_filepath, index=False, **save_kwargs)

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


# =============================================================================
# AppendSQLDataset
# =============================================================================

class AppendSQLDataset(AbstractDataset):
    """
    Dataset que grava em tabela PostgreSQL seguindo a mesma semântica de
    deduplicação do AppendCSVDataset.

    Estratégias por environment:
        prd        — upsert nativo PostgreSQL:
                     INSERT ... ON CONFLICT (pk_columns) DO UPDATE SET ...
                     Apenas as linhas novas/alteradas tocam o banco.
        sandbox    — lógica espelho do CSV: lê a tabela sandbox.<table>,
                     concatena, deduplica e reescreve via TRUNCATE + INSERT.
        dev / test — igual sandbox (aponta para sandbox.<table>).
        hk         — igual sandbox.

    Parâmetros do catalog.yml
    ─────────────────────────
    table_name  : "schema.table"  (ex: "stage.stage_cds")
    credentials : chave de credentials.yml que contenha {"con": "<conn_str>"}
    pk_columns  : lista de colunas que formam a PK natural para dedup/upsert
    environment : prd | sandbox | dev | test | hk  (default: prd)
    load_args   : kwargs extras para pd.read_sql  (ex: parse_dates, chunksize)
    save_args   : kwargs extras para DataFrame.to_sql  (ex: dtype, method)
    batch_size  : nº de linhas por batch no upsert prd  (default: 1000)
    """

    _SANDBOX_ENVS = frozenset({"dev", "test", "hk", "sandbox"})

    def __init__(
        self,
        table_name: str,
        credentials: Dict[str, Any],
        pk_columns: List[str],
        environment: str = "prd",
        load_args: Optional[Dict[str, Any]] = None,
        save_args: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> None:
        self._table_name = table_name
        self._credentials = credentials
        self._pk_columns = list(pk_columns)
        self.environment = environment
        self._load_args: Dict[str, Any] = load_args or {}
        self._save_args: Dict[str, Any] = save_args or {}
        self._batch_size = batch_size

        super().__init__()

        parts = table_name.split(".")
        if len(parts) == 2:
            self._schema, self._table = parts
        else:
            self._schema = "public"
            self._table = parts[0]

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _resolve_schema(self) -> str:
        """Retorna o schema alvo levando em conta o environment."""
        return "sandbox" if self.environment in self._SANDBOX_ENVS else self._schema

    def _build_engine(self) -> Engine:
        con: str = self._credentials.get("con", "")
        if not con:
            raise ValueError(
                "AppendSQLDataset: 'con' não encontrado nas credentials. "
                "Verifique conf/local/credentials.yml."
            )
        return create_engine(
            con,
            pool_pre_ping=True,   # detecta conexões mortas automaticamente
            pool_size=2,
            max_overflow=3,
        )

    @contextmanager
    def _engine(self) -> Generator[Engine, None, None]:
        """Context manager que garante dispose do engine após uso."""
        engine = self._build_engine()
        try:
            yield engine
        finally:
            engine.dispose()

    # -------------------------------------------------------------------------
    # AbstractDataset interface
    # -------------------------------------------------------------------------

    def _load(self) -> pd.DataFrame:
        schema = self._resolve_schema()
        with self._engine() as engine:
            if not inspect(engine).has_table(self._table, schema=schema):
                logger.warning(
                    "AppendSQLDataset._load: tabela %s.%s não existe — retornando DataFrame vazio.",
                    schema, self._table,
                )
                return pd.DataFrame()

            query = f'SELECT * FROM "{schema}"."{self._table}"'
            try:
                return pd.read_sql(query, engine, **self._load_args)
            except Exception as exc:
                logger.error(
                    "AppendSQLDataset._load falhou em %s.%s: %s",
                    schema, self._table, exc,
                )
                raise

    def _save(self, data: pd.DataFrame) -> None:
        schema = self._resolve_schema()
        data = SchemaRegistry._apply_schema(data, name=self._table)

        if self.environment in self._SANDBOX_ENVS:
            self._save_sandbox(data, schema)
        else:
            self._save_upsert(data, schema)

    def _exists(self) -> bool:
        schema = self._resolve_schema()
        with self._engine() as engine:
            try:
                return inspect(engine).has_table(self._table, schema=schema)
            except Exception:
                return False

    def _describe(self) -> Dict[str, Any]:
        return {
            "table_name": self._table_name,
            "environment": self.environment,
            "pk_columns": self._pk_columns,
            "batch_size": self._batch_size,
        }

    # -------------------------------------------------------------------------
    # Estratégia prd: upsert nativo PostgreSQL
    # -------------------------------------------------------------------------

    def _save_upsert(self, data: pd.DataFrame, schema: str) -> None:
        """
        INSERT ... ON CONFLICT (pk_columns) DO UPDATE SET <non_pk_cols>
        Processa em batches para controlar uso de memória.
        """
        if data.empty:
            logger.warning("AppendSQLDataset._save_upsert: DataFrame vazio, nada a inserir.")
            return

        with self._engine() as engine:
            from sqlalchemy import MetaData, Table

            meta = MetaData()
            meta.reflect(bind=engine, schema=schema, only=[self._table])
            tbl_key = f"{schema}.{self._table}"
            if tbl_key not in meta.tables:
                raise ValueError(
                    f"AppendSQLDataset: tabela '{tbl_key}' não encontrada no banco. "
                    "Verifique se os scripts de inicialização foram executados."
                )
            table: Table = meta.tables[tbl_key]

            non_pk = [c for c in data.columns if c not in self._pk_columns]
            total = 0

            with engine.begin() as conn:
                for start in range(0, len(data), self._batch_size):
                    batch = data.iloc[start:start + self._batch_size]
                    records = batch.where(pd.notna(batch), None).to_dict(orient="records")

                    stmt = pg_insert(table).values(records)
                    if non_pk:
                        stmt = stmt.on_conflict_do_update(
                            index_elements=self._pk_columns,
                            set_={col: getattr(stmt.excluded, col) for col in non_pk},
                        )
                    else:
                        stmt = stmt.on_conflict_do_nothing(index_elements=self._pk_columns)

                    conn.execute(stmt)
                    total += len(records)

            logger.info(
                "AppendSQLDataset.upsert → %s.%s: %d linhas processadas.",
                schema, self._table, total,
            )

    # -------------------------------------------------------------------------
    # Estratégia sandbox/dev/test/hk: ler → concat → dedup → TRUNCATE + INSERT
    # -------------------------------------------------------------------------

    def _save_sandbox(self, data: pd.DataFrame, schema: str) -> None:
        """
        Replica a lógica do AppendCSVDataset:
          1. Lê todos os dados existentes da tabela sandbox
          2. Concatena com os novos dados
          3. Deduplica pela PK (mantém o mais recente)
          4. TRUNCATE + INSERT (idempotente)
        """
        existing = self._load()
        combined = pd.concat([existing, data], ignore_index=True)
        combined = self._dedup(combined)

        if combined.empty:
            logger.warning("AppendSQLDataset._save_sandbox: combined vazio, nada a gravar.")
            return

        with self._engine() as engine:
            with engine.begin() as conn:
                conn.execute(text(f'TRUNCATE TABLE "{schema}"."{self._table}"'))
                combined.where(pd.notna(combined), None).to_sql(
                    self._table,
                    conn,
                    schema=schema,
                    if_exists="append",
                    index=False,
                    chunksize=self._batch_size,
                    **self._save_args,
                )

        logger.info(
            "AppendSQLDataset.sandbox → %s.%s: %d linhas gravadas.",
            schema, self._table, len(combined),
        )

    # -------------------------------------------------------------------------
    # Deduplicação (espelha AppendCSVDataset._save)
    # -------------------------------------------------------------------------

    def _dedup(self, combined: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicatas pela PK configurada, mantendo o registro mais recente
        (último na ordem da PK — equivalente ao keep='last' do AppendCSVDataset).
        """
        if combined.empty or not self._pk_columns:
            return combined

        valid_keys = [c for c in self._pk_columns if c in combined.columns]
        if not valid_keys:
            logger.warning(
                "AppendSQLDataset._dedup: pk_columns %s ausentes no DataFrame. "
                "Retornando sem dedup.",
                self._pk_columns,
            )
            return combined

        combined = combined.sort_values(valid_keys, ascending=False)
        combined = combined.drop_duplicates(subset=valid_keys, keep="first")
        return combined
