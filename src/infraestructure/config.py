"""Configuração centralizada do ISMB via variáveis de ambiente.

Usa pydantic-settings para validação, type safety e .env loading.
Todas as configs do sistema são acessiveis via Settings().
"""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    """Configurações do Apache Kafka."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    consumer_group: str = "ismb-streaming"
    auto_offset_reset: str = "earliest"


class MinIOSettings(BaseSettings):
    """Configurações do MinIO."""

    model_config = SettingsConfigDict(env_prefix="MINIO_")

    endpoint: str = "http://localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    bucket: str = "lakehouse"


class RedisSettings(BaseSettings):
    """Configurações do Redis."""

    model_config = SettingsConfigDict(env_prefix="REDIS_")

    url: str = "http://localhost:6379/0"
    ismb_key: str = "ismb:current"
    channel: str = "ismb:live"
    history_prefix: str = "ismb:history:"


class SparkSettings(BaseSettings):
    """Configurações do Apache Spark."""

    model_config = SettingsConfigDict(env_prefix="SPARK_")

    master_url: str = "spark://localhost:7077"
    app_name: str = "ismb-streaming"
    checkpoint_dir: str = "s3a://lakehouse/checkpoints"


class CollectorSettings(BaseSettings):
    """Configurações do coletores de dados."""

    model_config = SettingsConfigDict(env_prefix="")

    market_poll_interval_seconds: float = 15.0
    news_poll_interval_seconds: float = 120.0
    simulator_tweets_poll_interval_seconds: float = 2.0


class APISettings(BaseSettings):
    """Configurações do backend API FastAPI."""

    model_config = SettingsConfigDict(env_prefix="")

    jwt_secret_key: str = "change_me_use_openssll_rand_hex_32"
    jwt_algorithm: str = "HS256"
    jwt_acess_token_expire_minutes: int = 30
    jwt_refresh_token_expire_days: int = 7
    backend_cors_origins: str = "http://localhost:3000"


class ISMBSettings(BaseSettings):
    """Configurações calculo do ISMB."""

    model_config = SettingsConfigDict(env_prefix="ISMB_")

    calibration_window_days: int = 252
    calculation_window_seconds: int = 30


class Settings(BaseSettings):
    """Configurações gerais do ISMB."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        )

    kafka: KafkaSettings = KafkaSettings()
    minio: MinIOSettings = MinIOSettings()
    redis: RedisSettings = RedisSettings()
    spark: SparkSettings = SparkSettings()
    collector: CollectorSettings = CollectorSettings()
    api: APISettings = APISettings()
    ismb: ISMBSettings = ISMBSettings()

    log_format: str = "console"
