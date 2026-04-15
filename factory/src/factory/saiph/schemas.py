"""Registro central de schemas das tabelas stage.

Define os tipos esperados por tabela e aplica coercao automatica
de tipos ao salvar um DataFrame no catalogo.
"""
import logging
import pandas as pd

SCHEMA_API_FINANCE = {
    "dat_ref": "string",
    "close_adj_price": "float",
    "close_price": "float",
    "high_price": "float",
    "low_price": "float",
    "open_price": "float",
    "volume": "Int64",
}

SCHEMA_NEWS = {
    "id_news": "string",
    "dat_ref": "string",
    "fonte": "string",
    "titulo": "string",
    "link": "string",
}

SCHEMA_WEB_SCRAPING = {
    "dat_ref": "string",
    "open_price": "float",
    "close_price": "float",
    "high_price": "float",
    "low_price": "float",
    "change_percentage": "float",
}

logger = logging.getLogger(__name__)


class SchemaRegistry:
    """Mapeamento de schemas por nome de tabela.

    Os schemas definem as colunas e os tipos esperados.
    Usados para validar e converter DataFrames antes de persisti-los.
    """

    _schemas: dict = {
        "stage_ibov": SCHEMA_API_FINANCE,
        "stage_ivvb": SCHEMA_API_FINANCE,
        "stage_infomoney": SCHEMA_NEWS,
        "stage_valorinveste": SCHEMA_NEWS,
        "stage_seudinheiro": SCHEMA_NEWS,
        "stage_moneytimes": SCHEMA_NEWS,
        "stage_cds": SCHEMA_WEB_SCRAPING,
        "stage_ifix": SCHEMA_WEB_SCRAPING,
    }

    @classmethod
    def get_schema(cls, name: str, default: dict | None = None) -> dict | None:
        """Retorna o schema pelo nome da tabela, ou default se nao encontrado."""
        return cls._schemas.get(name, default)

    @classmethod
    def _apply_schema(cls, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """Aplica o schema ao DataFrame: valida colunas e converte tipos.

        Levanta ValueError se alguma coluna definida no schema estiver ausente.
        Colunas extras no DataFrame sao descartadas, e a ordem segue o schema.
        """
        schema = cls.get_schema(name=name, default=None)

        if schema is None:
            return df

        colunas_schema = list(schema.keys())
        colunas_df = list(df.columns)
        missing_schema_cols = [col for col in colunas_schema if col not in colunas_df]

        if cls._diff_list(colunas_schema, colunas_df):
            logger.warning(
                "Colunas divergentes entre DataFrame e schema: %s",
                cls._diff_list(colunas_df, colunas_schema),
            )
            logger.warning("Colunas do DataFrame: %s", colunas_df)
            logger.warning("Colunas do Schema: %s", colunas_schema)

        if missing_schema_cols:
            raise ValueError(
                "Colunas do schema ausentes no DataFrame: %s" % missing_schema_cols
            )

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

        return df[colunas_schema]

    @classmethod
    def _diff_list(cls, list1: list, list2: list) -> list:
        """Retorna os elementos presentes em apenas uma das listas (diferenca simetrica)."""
        return list(set(list1).symmetric_difference(set(list2)))
