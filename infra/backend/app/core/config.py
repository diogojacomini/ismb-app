"""
Application Configuration Module.

Define configurações centralizadas para conexão com banco de dados PostgreSQL.
Todas as configurações podem ser sobrescritas via variáveis de ambiente.

Classes:
    DatabaseConfig: Credenciais e limites do pool PostgreSQL

Example:
    from app.core.config import DatabaseConfig

    # Usar configuração padrão
    conn_str = DatabaseConfig.CONNECTION_STRING

    # Sobrescrever via environment
    # export DATABASE_URL="postgresql://user:pass@prod:5432/ismb"
"""

import os


class DatabaseConfig:
    """
    Configuração de conexão PostgreSQL.

    Todas as configurações podem ser sobrescritas via variáveis de ambiente,
    permitindo diferentes configurações por ambiente (dev, staging, prod).

    Attributes:
        CONNECTION_STRING: String de conexão PostgreSQL (ENV: DATABASE_URL)
        MIN_CONNECTIONS: Conexões mínimas no pool (ENV: DB_MIN_CONNECTIONS)
        MAX_CONNECTIONS: Conexões máximas no pool (ENV: DB_MAX_CONNECTIONS)
        QUERY_TIMEOUT: Timeout de queries em segundos (ENV: DB_QUERY_TIMEOUT)

    Example:
        # Usar padrão
        db_url = DatabaseConfig.CONNECTION_STRING

        # Sobrescrever via environment
        export DATABASE_URL="postgresql://user:pass@prod-server:5432/ismb"
    """

    # Default connection string (can be overridden by environment variable)
    CONNECTION_STRING = os.getenv(
        "DATABASE_URL", "postgresql+psycopg2://ismb:ismb@localhost:5433/ismb_data"
    )

    # Connection pool settings
    MIN_CONNECTIONS = int(os.getenv("DB_MIN_CONNECTIONS", "2"))
    MAX_CONNECTIONS = int(os.getenv("DB_MAX_CONNECTIONS", "10"))

    # Query timeout in seconds
    QUERY_TIMEOUT = int(os.getenv("DB_QUERY_TIMEOUT", "30"))
