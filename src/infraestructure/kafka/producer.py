"""Kafka Producer wrapper com serialização JSON e delivery reports.

Encapsula confluent_kafka.Producer com:
- Serialização automatica JSON UTF-8
- Delivery reports via structlog
- Retry em BufferError
- Métricas de mensagens enviadas/falhas
"""

from __future__ import annotations

import json
from typing import Any
import structlog
from confluent_kafka import Producer

logger = structlog.get_logger()


class KafkaProducerWrapper:

    def __init__(self, config: dict[str, Any]) -> None:
        self._producer = Producer(config)
        self._sent = 0
        self._errors = 0

    def send(self, topic: str, value: dict[str, Any], key: str | None = None) -> None:
        """Serializa e envia mensagem para o kafka.

        Args:
            topic: Nome do topico Kafka.
            value: Dicionário a ser serializado como JSON.
            key: Chave de particionamento (opcional)
        """
        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8") if key else None,
                value=json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"),
                callback=self._delivery_report,
            )
            self._producer.poll(0)
        except BufferError:
            logger.warning("kafka_buffer_full", topic=topic)
            self._producer.poll(1)
            self.send(topic, value, key)

    def flush(self, timeout: float = 10.0) -> None:
        """Flush de mensagens pendentes."""
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning("kafka_flush_incomplete", remaining=remaining)

    def close(self) -> None:
        """Flush e fecha o producer"""
        self.flush()

    def _delivery_report(self, err: Any, msg: Any) -> None:
        """Callback de delivery report."""
        if err:
            self._errors += 1
            logger.error("kafka_delivery_failed", topic=msg.topic(), error=str(err))
        else:
            self._sent += 1

    @property
    def stats(self) -> dict[str, int]:
        """Métricas do producer."""
        return {"sent": self._sent, "errors": self._errors}
