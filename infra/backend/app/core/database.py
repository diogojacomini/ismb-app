"""
Gerenciamento de conexões de banco de dados.
"""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class DatabasePool:
    """
    PostgreSQL connection pool manager.
    """

    _instance: Optional["DatabasePool"] = None
    _pool: Optional[pool.ThreadedConnectionPool] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def initialize(
        self, connection_string: str, minconn: int = 2, maxconn: int = 10
    ) -> None:
        """
        Inicializa o pool de conexões.

        Args:
            connection_string: String de conexão do PostgreSQL
            minconn: Número mínimo de conexões a serem mantidas
            maxconn: Número máximo de conexões permitidas
        """
        if self._pool is not None:
            logger.warning("DatabasePool already initialized")
            return

        try:
            # Parse connection string to extract components
            # Format: postgresql://user:password@host:port/database
            if connection_string.startswith("postgresql+psycopg2://"):
                connection_string = connection_string.replace(
                    "postgresql+psycopg2://", "postgresql://"
                )

            self._pool = pool.ThreadedConnectionPool(
                minconn, maxconn, connection_string
            )
            logger.info(f"Database pool initialized (min={minconn}, max={maxconn})")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise

    @contextmanager
    def get_connection(self) -> Iterator[psycopg2.extensions.connection]:
        """
        Context manager to get a connection from the pool.

        Yields:
            A psycopg2 connection object

        Example:
            with db_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM table")
        """
        if self._pool is None:
            raise RuntimeError("DatabasePool not initialized. Call initialize() first.")

        conn = None
        try:
            conn = self._pool.getconn()
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                self._pool.putconn(conn)

    @contextmanager
    def get_cursor(
        self, cursor_factory=RealDictCursor
    ) -> Iterator[psycopg2.extensions.cursor]:
        """
        Context manager to get a cursor with automatic connection management.

        Args:
            cursor_factory: Cursor factory to use (default: RealDictCursor for dict results)

        Yields:
            A psycopg2 cursor object

        Example:
            with db_pool.get_cursor() as cur:
                cur.execute("SELECT * FROM table")
                rows = cur.fetchall()  # Returns list of dicts
        """
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()

    def close(self) -> None:
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()
            self._pool = None
            logger.info("Database pool closed")


# Global singleton instance
db_pool = DatabasePool()


# ── query helpers ──────────────────────────────────────────────────────────


def execute_query(
    query: str, params: Optional[tuple | dict] = None, fetch: bool = True
) -> List[Dict[str, Any]]:
    """
    Execute a SQL query and return results as list of dicts.

    Args:
        query: SQL query string (use %s or %(name)s for parameterization)
        params: Query parameters (tuple for positional, dict for named)
        fetch: Whether to fetch results (False for INSERT/UPDATE/DELETE)

    Returns:
        List of dictionaries (one per row) or empty list

    Example:
        rows = execute_query("SELECT * FROM table WHERE id = %s", (123,))
        rows = execute_query("SELECT * FROM table WHERE name = %(name)s", {"name": "test"})
    """
    with db_pool.get_cursor() as cur:
        cur.execute(query, params)
        if fetch:
            return [dict(row) for row in cur.fetchall()]
        return []


def fetch_one(
    query: str, params: Optional[tuple | dict] = None
) -> Optional[Dict[str, Any]]:
    """
    Execute a query and return a single row as dict.

    Args:
        query: SQL query string
        params: Query parameters

    Returns:
        Dictionary or None if no results
    """
    with db_pool.get_cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else None
