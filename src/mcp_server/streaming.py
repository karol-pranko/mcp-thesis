#!/usr/bin/env python3
"""
Konsument Kafki z telemetrią IoT. Słucha tematu w tle, odkłada zdarzenia
CRITICAL_ANOMALY do bufora i powiadamia subskrybentów (push przez SSE).
"""

import asyncio
from collections import deque
import json
import logging
import os
import sys
from typing import Any, Callable, Dict, List, Optional

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger("mcp_streaming")


class KafkaTelemetryConsumer:

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        group_id: Optional[str] = None,
        buffer_size: int = 100,
    ) -> None:
        # domyślne wartości pasują do testbed/docker-compose.yml
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"
        )
        self.topic = topic or os.getenv("KAFKA_TOPIC", "telemetry_test")
        self.group_id = group_id or os.getenv("KAFKA_GROUP_ID", "mcp_stream_group")
        self.buffer_size = buffer_size

        self.anomalies_buffer: deque[Dict[str, Any]] = deque(maxlen=buffer_size)
        self.latest_anomaly: Optional[Dict[str, Any]] = None
        self._listeners: List[Callable[[Dict[str, Any]], Any]] = []

        self.consumer: Optional[AIOKafkaConsumer] = None
        self.consumer_task: Optional[asyncio.Task] = None
        self.running: bool = False

        self.total_events_received: int = 0
        self.total_anomalies_detected: int = 0

    async def start(self) -> None:
        if self.running:
            logger.warning("Konsument Kafka jest już uruchomiony.")
            return

        self.running = True
        logger.info(
            "Uruchamianie konsumenta Kafka na brokerze %s (temat: '%s', grupa: '%s')...",
            self.bootstrap_servers,
            self.topic,
            self.group_id,
        )

        # nie czekamy na połączenie z brokerem - serwer ma wstać nawet bez Kafki
        self.consumer_task = asyncio.create_task(
            self._consume_loop(), name="mcp_kafka_consumer_loop"
        )

    async def _consume_loop(self) -> None:
        """Pętla z reconnectem co 2 s - broker potrafi wstać później niż serwer."""
        retry_delay_seconds = 2.0

        while self.running:
            try:
                logger.info(
                    "Nawiązywanie połączenia z Apache Kafka na adresie: %s...",
                    self.bootstrap_servers,
                )
                self.consumer = AIOKafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    value_deserializer=lambda raw_bytes: json.loads(raw_bytes.decode("utf-8")),
                )
                await self.consumer.start()
                logger.info(
                    "Konsument Kafka pomyślnie zasubskrybował temat '%s'. Rozpoczęto nasłuch zdarzeń.",
                    self.topic,
                )

                async for record in self.consumer:
                    if not self.running:
                        break

                    self.total_events_received += 1
                    event_payload = record.value

                    if isinstance(event_payload, dict):
                        status = event_payload.get("status")
                        if status == "CRITICAL_ANOMALY":
                            self.record_anomaly(event_payload)

                            device_id = event_payload.get("device_id", "NIEZNANE")
                            temp = event_payload.get("temperature", 0.0)
                            press = event_payload.get("pressure", 0.0)
                            event_id = event_payload.get("event_id", "N/A")

                            logger.warning(
                                "WYKRYTO ANOMALIĘ KRYTYCZNĄ: Urządzenie %s | Temp: %.2f°C | Ciśnienie: %.2f hPa | Event ID: %s",
                                device_id,
                                temp,
                                press,
                                event_id,
                            )

            except asyncio.CancelledError:
                logger.info("Zadanie konsumenta Kafka zostało anulowane.")
                break
            except KafkaError as kafka_err:
                logger.warning(
                    "Błąd komunikacji z Apache Kafka: %s. Ponowna próba za %.1f s...",
                    kafka_err,
                    retry_delay_seconds,
                )
                await asyncio.sleep(retry_delay_seconds)
            except Exception as exc:
                logger.error(
                    "Nieoczekiwany wyjątek w pętli konsumenta Kafka: %s. Ponowna próba za %.1f s...",
                    exc,
                    retry_delay_seconds,
                    exc_info=True,
                )
                await asyncio.sleep(retry_delay_seconds)
            finally:
                if self.consumer is not None:
                    try:
                        await self.consumer.stop()
                    except Exception:
                        pass
                    self.consumer = None

    def get_latest_anomalies(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Najnowsze pierwsze."""
        if not self.anomalies_buffer:
            return []
        all_buffered = list(self.anomalies_buffer)
        return all_buffered[-limit:][::-1]

    def get_latest_anomaly(self) -> Optional[Dict[str, Any]]:
        return self.latest_anomaly

    def get_latest_anomaly_json(self) -> str:
        if self.latest_anomaly is not None:
            return json.dumps(self.latest_anomaly, ensure_ascii=False, indent=2)

        return json.dumps(
            {
                "status": "NO_ANOMALY_RECORDED",
                "message": "Brak zarejestrowanych krytycznych anomalii telemetrycznych w bieżącej sesji serwera MCP.",
                "total_events_received": self.total_events_received,
                "total_anomalies_detected": self.total_anomalies_detected,
            },
            ensure_ascii=False,
            indent=2,
        )

    def add_anomaly_listener(self, listener: Callable[[Dict[str, Any]], Any]) -> None:
        """Listener może być zwykłą funkcją albo korutyną."""
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_anomaly_listener(self, listener: Callable[[Dict[str, Any]], Any]) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    def record_anomaly(self, event_payload: Dict[str, Any]) -> None:
        self.total_anomalies_detected += 1
        self.latest_anomaly = event_payload
        self.anomalies_buffer.append(event_payload)

        # kopia listy - listener może się wyrejestrować w trakcie
        for listener in list(self._listeners):
            try:
                res = listener(event_payload)
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception as listener_exc:
                logger.error("Błąd podczas powiadamiania listenera anomalii: %s", listener_exc)

    async def stop(self) -> None:
        logger.info("Zatrzymywanie konsumenta telemetrycznego Kafka...")
        self.running = False

        if self.consumer_task and not self.consumer_task.done():
            self.consumer_task.cancel()
            try:
                await asyncio.wait_for(self.consumer_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            self.consumer_task = None

        if self.consumer is not None:
            try:
                await self.consumer.stop()
            except Exception as stop_exc:
                logger.warning("Błąd podczas zamykania konsumenta AIOKafka: %s", stop_exc)
            self.consumer = None

        logger.info(
            "Konsument telemetryczny Kafka został zatrzymany (odebrano łącznie: %d zdarzeń, wykryto: %d anomalii).",
            self.total_events_received,
            self.total_anomalies_detected,
        )


streaming_consumer = KafkaTelemetryConsumer()
