"""
Datasets customizados para particionamento automático por odate e append em CSV/SQL.

AppendCSVDataset  - grava em arquivo CSV com deduplicação por chave natural.
AppendSQLDataset  - grava em tabela PostgreSQL com upsert (prd) ou
                    truncate+insert (sandbox/dev/test/hk), replicando a mesma
                    lógica de deduplicação do AppendCSVDataset.
"""
import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

import fsspec
import pandas as pd
from kedro.io import AbstractDataset
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

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
        prd        - upsert nativo PostgreSQL:
                     INSERT ... ON CONFLICT (pk_columns) DO UPDATE SET ...
                     Apenas as linhas novas/alteradas tocam o banco.
        sandbox    - lógica espelho do CSV: lê a tabela sandbox.<table>,
                     concatena, deduplica e reescreve via TRUNCATE + INSERT.
        dev / test - igual sandbox (aponta para sandbox.<table>).
        hk         - igual sandbox.

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

        # Fallback: se a credential não foi carregada corretamente (e.g. conf_source
        # errado no Airflow), tenta a variável de ambiente ISMB_DB_CONN que é
        # injetada pelo docker-compose com o hostname correto (ismb-db:5432).
        if not con:
            con = os.environ.get("ISMB_DB_CONN", "")

        if not con:
            raise ValueError(
                "AppendSQLDataset: 'con' não encontrado nas credentials nem em "
                "ISMB_DB_CONN. Verifique conf/airflow/credentials.yml ou o "
                "docker-compose.yaml."
            )

        # Aviso de segurança: localhost dentro de container Docker não funciona.
        if "localhost" in con or "127.0.0.1" in con:
            env_con = os.environ.get("ISMB_DB_CONN", "")
            if env_con:
                logger.warning(
                    "AppendSQLDataset: connection string aponta para 'localhost' "
                    "mas estamos dentro de um container. Usando ISMB_DB_CONN=%s "
                    "como fallback seguro.",
                    env_con,
                )
                con = env_con

        # NullPool: sem pooling de conexões — cada operação abre e fecha a
        # conexão de forma independente. Elimina conexões obsoletas/ociosas
        # que causavam OperationalError após a migração para SQL.
        return create_engine(
            con,
            poolclass=NullPool,
            echo=False,
            future=True,
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
                    "AppendSQLDataset._load: tabela %s.%s não existe - retornando DataFrame vazio.",
                    schema, self._table,
                )
                return pd.DataFrame()

            query = text(f'SELECT * FROM "{schema}"."{self._table}"')
            try:
                # pd.read_sql não é compatível com SQLAlchemy 2.0 (future=True):
                # chama .cursor() que não existe em Connection 2.0.
                # Solução definitiva: executar via API nativa do SQLAlchemy e
                # construir o DataFrame manualmente.
                with engine.connect() as conn:
                    result = conn.execute(query)
                    df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))

                # psycopg2 retorna tipos Decimal/UUID/etc. para colunas numéricas
                # do PostgreSQL, que pandas mapeia como dtype=object. Coerce
                # colunas object para numeric onde possível, preservando strings.
                for col in df.columns:
                    if df[col].dtype == object:
                        converted = pd.to_numeric(df[col], errors="ignore")
                        if converted.dtype != object:
                            df[col] = converted

                # Aplica parse_dates de load_args manualmente, se presente
                parse_dates = self._load_args.get("parse_dates")
                if parse_dates:
                    for col in (parse_dates if isinstance(parse_dates, list) else list(parse_dates)):
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], errors="coerce")

                return df
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

            # SQLAlchemy 2.0 (future=True): meta.reflect requer Connection, não Engine.
            meta = MetaData()
            with engine.connect() as reflect_conn:
                meta.reflect(bind=reflect_conn, schema=schema, only=[self._table])

            tbl_key = f"{schema}.{self._table}"
            if tbl_key not in meta.tables:
                raise ValueError(
                    f"AppendSQLDataset: tabela '{tbl_key}' não encontrada no banco. "
                    "Verifique se os scripts de inicialização foram executados."
                )
            table: Table = meta.tables[tbl_key]

            # Identifica colunas JSONB para sanitização especial de NaN.
            # pd.notna() não captura string 'NaN', apenas float NaN —
            # e PostgreSQL rejeita 'NaN'::JSONB como JSON inválido.
            from sqlalchemy.dialects.postgresql import JSONB as SA_JSONB
            jsonb_cols = {c.name for c in table.columns if isinstance(c.type, SA_JSONB)}

            non_pk = [c for c in data.columns if c not in self._pk_columns]
            total = 0

            with engine.begin() as conn:
                for start in range(0, len(data), self._batch_size):
                    batch = data.iloc[start:start + self._batch_size]
                    records = batch.where(pd.notna(batch), None).to_dict(orient="records")

                    # Sanitiza colunas JSONB: substitui float('nan') e string 'NaN'
                    # por None (que vira null em JSON — válido no PostgreSQL).
                    if jsonb_cols:
                        import math
                        for rec in records:
                            for col in jsonb_cols:
                                if col not in rec:
                                    continue
                                val = rec[col]
                                if val is None:
                                    continue
                                if isinstance(val, float) and math.isnan(val):
                                    rec[col] = None
                                elif isinstance(val, str) and val.strip().lower() == 'nan':
                                    rec[col] = None

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
                "AppendSQLDataset.upsert -> %s.%s: %d linhas processadas.",
                schema, self._table, total,
            )

    # -------------------------------------------------------------------------
    # Estratégia sandbox/dev/test/hk: ler -> concat -> dedup -> TRUNCATE + INSERT
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
            
            # Passa engine em vez de connection para evitar warning do pandas
            combined.where(pd.notna(combined), None).to_sql(
                self._table,
                engine,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=self._batch_size,
                **self._save_args,
            )

        logger.info(
            "AppendSQLDataset.sandbox -> %s.%s: %d linhas gravadas.",
            schema, self._table, len(combined),
        )

    # -------------------------------------------------------------------------
    # Deduplicação (espelha AppendCSVDataset._save)
    # -------------------------------------------------------------------------

    def _dedup(self, combined: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicatas pela PK configurada, mantendo o registro mais recente
        (último na ordem da PK - equivalente ao keep='last' do AppendCSVDataset).
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
