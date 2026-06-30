"""Bronze Ingestion - Kafka para Bronze Delta Lake (append-only).

Etapa 1 do pipeline Spark Straming.
Dados exatamente como chegam do Kafka são persistidos.

Referências: EXECUTION_FLOW.md (2.3 - Etapa 1)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, date_format
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType


# Schema do Market no Kafka (JSON)
MARKET_TICKER_SCHEMA = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("timestamp", StringType(), True),
])


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
) -> DataFrame:
    """Lê um tópico Kafka como DataFrame Spark Streaming.

    Args:
        spark (SparkSession): SparkSession ativa.
        bootstrap_servers (str): Servidor Kafka.
        topic (str): Nome do tópico Kafka.

    Returns:
        DataFrame com colunas key, value, topic, partition, offset, timestamp.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def bronze_market_ingestion(
    kafka_df: DataFrame,
) -> DataFrame:
    """Transforma o DataFrame Kafka em DataFrame Bronze Market.

    Args:
        kafka_df (DataFrame): DataFrame Kafka com colunas key, value, topic, partition, offset, timestamp.

    Returns:
        DataFrame Bronze Market com colunas ticker, price, volume, timestamp.
    """
    return (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_value",
                    "timestamp as kafka_ts",
                    "partition as kafka_partition",
                    "offset as kafka_offset")
        .withColumn("data", from_json(col("json_value"), MARKET_TICKER_SCHEMA))
        .select(
            col("data.ticker").alias("ticker"),
            col("data.price").alias("price"),
            col("data.volume").alias("volume"),
            col("data.timestamp").alias("event_timestamp"),
            col("kafka_ts"),
            col("kafka_partition"),
            col("kafka_offset")
        )
        .withColumn(
            "dat_ref", date_format(col("event_timestamp"), "yyyy-MM-dd"))
    )


def write_bronze_stream(
    df: DataFrame,
    delta_path: str,
    checkpoint_path: str,
    trigger_interval: str = "10 seconds",
    partition_cols: list[str] | None = None,
) -> None:
    """Escreve stream em Delta Lake (append-only).

    Args:
        df (DataFrame): DataFrame Spark Streaming.
        delta_path (str): Local do Delta Lake (ex: s3a://lakehouse/bronze/market).
        checkpoint_path (str): Caminho do checkpint.
        trigger_interval (str): Intervalo de trigger do streaming (ex: "10 seconds").
        partition_cols (list[str] | None): Colunas para particionar o Delta Lake.
    """
    writer = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.start(delta_path)
