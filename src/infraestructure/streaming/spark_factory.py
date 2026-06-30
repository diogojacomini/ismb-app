"""Spark factory module - Criação de SparkSession para streaming.

Configura Delta Lake, MinIO e Kafka e otimização do Spark.
Padrão Factory: toda criação de SparkSession passa por aqui.

Referencia: CLEAN_ARCHITECTURE.md (3.4), execution_flow.md (2.3)
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from src.infraestructure.config import Settings


class SparkSessionFactory:
    """Factory para criar SparkSession configurada."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or Settings()

    def create(
        self,
        app_name: str | None = None,
        master: str | None = None,
    ) -> SparkSession:
        """Cria uma SparkSession com Delta Lake e MinIO.

        Args:
            app_name (str | None): Nome da aplicação Spark. Se None, usa o padrão da configuração.
            master (str | None): URL do master Spark. Se None, usa o padrão da configuração.

        Returns:
            SparkSession: SparkSession configurada.
        """
        s = self._settings
        _app_name = app_name or s.spark.app_name
        _master = master or s.spark.master_url

        builder = (
            SparkSession.builder
            .appName(_app_name)
            .master(_master)

            # Delta Lake
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )

            # MinIO
            .config("spark.hadoop.fs.s3a.endpoint", s.minio.endpoint)
            .config("spark.hadoop.fs.s3a.access.key", s.minio.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", s.minio.secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")

            # Kafka defaults
            .config(
                "spark.sql.streaming.useDeprecatedKafkaOffsetFetching",
                "false")

            # Otimização
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        )

        return builder.getOrCreate()

    def create_local(self, app_name: str = 'ismb-test') -> SparkSession:
        """Cria uma SparkSession local para testes.

        Args:
            app_name (str): Nome da aplicação Spark. Default: 'ismb-test'.

        Returns:
            SparkSession: SparkSession configurada para execução local.
        """
        return self.create(app_name=app_name, master="local[*]")
