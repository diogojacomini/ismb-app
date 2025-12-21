import pandas as pd
import logging

SCHEMA_API_FINANCE = {
    "dat_ref": "string",
    "close_adj": "float",
    "close": "float",
    "high": "float",
    "low": "float",
    "vpen": "float",
    "volume": "Int64",
}

SCHEMA_NEWS = {
    "dat_ref": "string",
    "fonte": "string",
    "titulo": "string",
    "link": "string",
}


logger = logging.getLogger(__name__)


class SchemaRegistry:
    _schemas = {
        "rw_ibov_stage": SCHEMA_API_FINANCE,
        "rw_ivvb_stage": SCHEMA_API_FINANCE,
        "rw_infomoney_stage": SCHEMA_NEWS,
        "rw_moneytimes_stage": SCHEMA_NEWS,
        "rw_seudinheiro_stage": SCHEMA_NEWS,
        "rw_valorinveste_stage": SCHEMA_NEWS,
    }

    @classmethod
    def get_schema(cls, name: str, default=None):
        return cls._schemas.get(name, default)

    @classmethod
    def _apply_schema(cls, df: pd.DataFrame, name: str) -> pd.DataFrame:
        schema = cls.get_schema(name=name, default=None)

        if schema is None:
            return df

        # Verificar se todas as colunas do schema estão presentes no DataFrame
        colunas_schema = list(schema.keys())
        colunas_df = list(df.columns)
        missing_schema_cols = [col for col in colunas_schema if col not in colunas_df]

        if cls._diff_list(colunas_schema, colunas_df):
            logger.warning(f"Existem colunas divergentes entre o DataFrame e o schema: {cls._diff_list(colunas_df, colunas_schema)}")
            logger.warning(f"Colunas do DataFrame: {colunas_df}")
            logger.warning(f"Colunas do Schema: {colunas_schema}")

        if missing_schema_cols:
            raise ValueError(f"As seguintes colunas do schema não foram encontradas no DataFrame: {missing_schema_cols}")

        # Aplica as conversões de tipo conforme o schema
        for col, dtype in schema.items():

            if dtype.lower() in ("float", "float64"):
                df[col] = pd.to_numeric(df[col], errors="coerce")

            elif dtype.lower() in ("int", "int64"):

                df[col] = pd.to_numeric(df[col], errors="coerce")
                try:
                    df[col] = df[col].astype("Int64")
                except Exception:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            elif dtype.lower().startswith("str") or dtype.lower() == "string":
                df[col] = df[col].fillna("").astype(str)

        # Retorna apenas as colunas definidas no schema, na ordem correta
        return df[colunas_schema]

    @classmethod
    def _diff_list(cls, list1, list2):
        list_l = set(list1).union(set(list2))
        list_r = set(list1).intersection(set(list2))
        return list(list_l - list_r)
