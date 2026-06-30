"""Entrypoint dos collectors - seleciona e inicia o collector correto.

Uso via Docker:
- COLLECTOR_TYPE=market python -m src.infraestructure.collectors.entrypoint
- COLLECTOR_TYPE=news python -m src.infraestructure.collectors.entrypoint
- COLLECTOR_TYPE=tweet python -m src.infraestructure.collectors.entrypoint
"""

from __future__ import annotations

import os
import sys
import structlog
from src.infraestructure.config import Settings
from src.infraestructure.kafka.producer import KafkaProducerWrapper

logger = structlog.get_logger()


def main() -> None:
    """Inicia o collector baseado na env COLLECTOR_TYPE."""
    collector_type = os.environ.get("COLLECTOR_TYPE", "market").lower()
    settings = Settings()

    producer = KafkaProducerWrapper({
        "bootstrap.servers": settings.kafka.bootstrap_servers,
        "client.id": f"ismb-collector-{collector_type}",
    })

    logger.info("collector_starting", type=collector_type)

    if collector_type == "market":
        from src.infraestructure.collectors.market_collector import MarketCollector
        collector = MarketCollector(
            producer=producer,
            poll_interval=settings.collector.market_poll_interval_seconds,
        )
        collector.start()

    else:
        logger.info("unknown_collector_type", type=collector_type)
        sys.exit(1)


if __name__ == "__main__":
    main()
