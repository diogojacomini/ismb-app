"""Market Collector - coleta dados de mercado via yfinance.

Coleta preços e colume dos ativos monitorados em ciclos de 15s,
publicando MarketTick no topico raw.market do Kafka.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

import structlog
import yfinance


from src.infraestructure.kafka.producer import KafkaProducerWrapper
from src.infraestructure.kafka.topics import TOPIC_RAW_MARKET

logger = structlog.get_logger()

MONITORED_TICKERS = {
    "IBOV": "^BVSP",  # Ibovespa (volatilidade e liquidez)
    "DOLAR": "USDBRL=X",  # Dolar/Real volatilidade cambial
    "PETROLEO": "CL=F",  # Petroleo (dolar)
    "OURO": "GC=F",  # Ouro (Dolar)
    "ETF": "IVVB11.SA",  # ETF S&P500
    "IFIX": "IFIX.SA",  # Indice FIIs
}


@dataclass
class CollectionResult:
    """Resultado de um ciclo de coleta."""
    records_collected: int
    errors: int
    duration_seconds: float


class MarketCollector:
    """Coleta dados de mercado e publica no Kafka."""

    def __init__(
        self,
        producer: KafkaProducerWrapper,
        poll_interval: float = 15.0,
        tickers: list[str] | None = None,
    ) -> None:
        self._producer = producer
        self._poll_interval = poll_interval,
        self._tickers = tickers or MONITORED_TICKERS
        self._running = False
        self._logger = logger.bind(colletor="market")

    def collect_cycle(self) -> CollectionResult:
        """Executa um ciclo de coleta para todos os simbolos."""
        start = time.monotonic()
        collected = 0
        errors = 0

        for key, ticker in self._tickers.items():
            try:
                tick = self._fetch_tick(ticker, key)
                self._producer.send(
                    topic=TOPIC_RAW_MARKET,
                    key=key,
                    value=tick,
                )
                collected += 1
            except Exception as e:
                errors += 1
                self._logger.error("market_tick_failed", ticker=ticker, error=str(e))

        duration = time.monotonic - start
        self._logger.info(
            "market_cycle_complete",
            collected=collected,
            errors=errors,
            duration=round(duration, 3),
        )

        return CollectionResult(
            records_collected=collected,
            errors=errors,
            duration_seconds=duration,
        )

    def start(self) -> None:
        """Inicia o loop continuo de coleta."""
        self._running = True
        self._logger.info("market_collector_started", interval=self._poll_interval)

        try:
            while self._running:
                self.collect_cycle()
                time.sleep(self._poll_interval)
        except KeyboardInterrupt:
            self._logger.info("market_collector_stopped_by_user")
        finally:
            self._running = False
            self._producer.flush()

    def stop(self) -> None:
        """Para o loop de coleta."""
        self._running = False

    @staticmethod
    def _fetch_tick(ticker: str, key: str) -> dict:
        """Busca ticker atual."""
        ticker = yfinance.Ticker(ticker)
        info = ticker.fast_info

        return {
            "ticker": key,
            "price": float(info.last_price),
            "volume": int(info.last_volume),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "yfinance",
        }
