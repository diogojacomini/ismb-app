"""Bronze Ingestion Job - Spark Streaming: Kafka para Bronze Delta Lake.

Job principal que consome tópicos Kafka (raw.market, raw.news, raw.tweets)
e persiste em Delta Lake Bronze (append-only)

Uso:
    spark-submit --master spark://localhost:7077 \
        src/infraestructure/streaming/jobs/bronze_ingestion_job.py

Referências: EXECUTION_FLOW.md (2.3 - Etapa 1)
"""
from __future__ import annotations
import structlog

from src.infraestructure.config import Settings
from src.infraestructure.streaming.spark_factory import SparkSessionFactory
from src.infraestructure.streaming.transformations.bronze_ingestion import (
    read_kafka_stream,
    bronze_market_ingestion,
    write_bronze_stream,
)

logger = structlog.get_logger()


def main() -> None:
    """Inicia os streams Bronze."""
    settings = Settings()
    factory = SparkSessionFactory(settings)
    spark = factory.create(app_name="ismb-bronze-ingestion")

    bootstrap = settings.kafka.bootstrap_servers
    bucket = settings.minio.bucket

    logger.info("bronze_ingestion_starting", bootstrap=bootstrap)

    # 1 - Market
    market_raw = read_kafka_stream(spark, bootstrap, "raw.market")
    market_bronze = bronze_market_ingestion(market_raw)
    write_bronze_stream(
        df=market_bronze,
        delta_path=f"s3a://{bucket}/bronze/market/",
        checkpoint_path=f"s3a://{bucket}/checkpoints/bronze_market",
        trigger_interval="10 seconds",
        partition_cols=["ticker", "dat_ref"],
    )

    logger.info("bronze_ingestion_all_streams_started")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

    # 5  export MINIO_ENDPOINT=http://minio:9000
    # 6  export KAFKA_BOOTSTRAP_SERVERS=kafka:29092
    # 8  export SPARK_MASTER_URL=spark://spark-master:7077
    # 9  /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/src/infraestructure/streaming/jobs/bronze_ingestion_job.py 
    
# {"ticker": "PETR4", "price":23.34, "volume":123000, "timestamp":"2026-02-29T20:15"}
# {"ticker": "DIOGO7", "price":17.06, "volume":223001, "timestamp":"2026-02-29T21:45"}