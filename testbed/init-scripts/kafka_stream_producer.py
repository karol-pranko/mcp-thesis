#!/usr/bin/env python3
"""
Producent sztucznej telemetrii IoT do Kafki - do testowania subskrypcji SSE w serwerze.
Około 3% zdarzeń to CRITICAL_ANOMALY. Na SIGINT/SIGTERM opróżnia bufor przed wyjściem.
"""

import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
import random
import signal
import sys
import time
from typing import Any, Dict, Optional
import uuid

from aiokafka import AIOKafkaProducer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("kafka_stream_producer")


# Konfiguracja producenta Kafki
@dataclass
class ProducerConfig:
    bootstrap_servers: str
    topic: str
    events_per_second: float
    anomaly_probability: float
    max_events: Optional[int] = None


def parse_command_line_arguments() -> ProducerConfig:
    """
    Parsuje parametry wiersza poleceń z wartościami domyślnymi:
    --bootstrap-servers: adres brokera Kafka (domyślnie 127.0.0.1:9092).
    --topic: nazwa tematu Kafki (domyślnie telemetry_test).
    --rate: częstotliwość generowania zdarzeń na sekundę (domyślnie 50).
    --anomaly-rate: prawdopodobieństwo wygenerowania anomalii (domyślnie 0.03).
    --max-events: opcjonalny limit liczby zdarzeń (przydatny w testach automatycznych).
    """
    parser = argparse.ArgumentParser(
        description="Asynchroniczny generator strumienia telemetrycznego do Apache Kafka dla MCP."
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        help="Adres brokera Kafka (domyślnie: 127.0.0.1:9092).",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=os.getenv("KAFKA_TOPIC", "telemetry_test"),
        help="Nazwa tematu w Apache Kafka (domyślnie: telemetry_test).",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=50.0,
        help="Liczba zdarzeń na sekundę (domyślnie: 50.0).",
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.03,
        help="Prawdopodobieństwo wstrzyknięcia anomalii krytycznej (domyślnie: 0.03 czyli ~3%%).",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Opcjonalna maksymalna liczba zdarzeń do wysłania (domyślnie brak limitu).",
    )

    args = parser.parse_args()
    return ProducerConfig(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        events_per_second=args.rate,
        anomaly_probability=args.anomaly_rate,
        max_events=args.max_events,
    )


def generate_telemetry_event(anomaly_probability: float) -> Dict[str, Any]:
    """
    Generuje pojedyncze zdarzenie telemetryczne w formacie JSON.
    Z prawdopodobieństwem określonym przez anomaly_probability wstrzykuje anomalię krytyczną
    (status CRITICAL_ANOMALY oraz skrajne odczyty temperatury i ciśnienia).
    """
    device_number = random.randint(1, 50)
    device_id = f"DEV_SENSOR_{device_number:03d}"
    event_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()

    is_anomaly = random.random() < anomaly_probability

    if is_anomaly:
        status = "CRITICAL_ANOMALY"
        # Skrajne wartości temperatury (> 120.0 C) i ciśnienia
        temperature = round(random.uniform(121.5, 158.0), 2)
        pressure = round(random.uniform(1150.0, 1380.0), 2)
    else:
        status = "OK"
        # Standardowy zakres pracy sensora (temperatura: 20.0 - 90.0 C, ciśnienie: 900.0 - 1100.0 hPa)
        temperature = round(random.uniform(20.0, 90.0), 2)
        pressure = round(random.uniform(900.0, 1100.0), 2)

    return {
        "event_id": event_id,
        "device_id": device_id,
        "timestamp": timestamp,
        "temperature": temperature,
        "pressure": pressure,
        "status": status,
    }


async def run_event_producer(config: ProducerConfig) -> None:
    """
    Główna pętla asynchronicznego producenta zdarzeń.
    Uruchamia klienta AIOKafkaProducer, wysyła zdarzenia ze zdefiniowaną częstotliwością
    oraz przechwytuje sygnały zakończenia (SIGINT/SIGTERM), dbając o opróżnienie bufora.
    """
    logger.info("Inicjalizacja producenta AIOKafka na adresie: %s...", config.bootstrap_servers)
    logger.info(
        "Parametry transmisji: temat = %s, częstotliwość = %.1f zdarzeń/s, anomalie = %.1f%%",
        config.topic,
        config.events_per_second,
        config.anomaly_probability * 100,
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=config.bootstrap_servers,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        acks="all",
        linger_ms=10,
    )

    await producer.start()
    logger.info("Producent AIOKafka został uruchomiony i połączony z klastrem.")

    shutdown_event = asyncio.Event()

    def handle_shutdown_signal() -> None:
        if not shutdown_event.is_set():
            logger.info("Odebrano sygnał zakończenia pracy (SIGINT/SIGTERM). Zatrzymywanie strumienia...")
            shutdown_event.set()

    current_loop = asyncio.get_running_loop()
    for signame in (signal.SIGINT, signal.SIGTERM):
        try:
            current_loop.add_signal_handler(signame, handle_shutdown_signal)
        except NotImplementedError:
            # add_signal_handler nie działa na Windowsie
            pass

    sleep_interval = 1.0 / max(config.events_per_second, 1.0)
    total_sent = 0
    total_anomalies = 0
    start_time = time.perf_counter()
    last_log_time = start_time

    try:
        while not shutdown_event.is_set():
            if config.max_events is not None and total_sent >= config.max_events:
                logger.info("Osiągnięto zaplanowany limit %d zdarzeń. Kończenie pracy...", config.max_events)
                break

            event = generate_telemetry_event(config.anomaly_probability)
            device_id = event["device_id"]

            if event["status"] == "CRITICAL_ANOMALY":
                total_anomalies += 1
                logger.warning(
                    "[ANOMALIA] Urządzenie: %s | Temp: %.1f C | Ciśnienie: %.1f hPa | ID: %s",
                    device_id,
                    event["temperature"],
                    event["pressure"],
                    event["event_id"],
                )

            # Asynchroniczne przekazanie komunikatu do bufora Kafki z kluczem partycjonowania
            await producer.send(config.topic, value=event, key=device_id)
            total_sent += 1

            # Cykliczny raport podsumowujący przepustowość co 5 sekund
            current_time = time.perf_counter()
            if current_time - last_log_time >= 5.0:
                elapsed_window = current_time - last_log_time
                current_rate = (total_sent - (total_sent - 50 * int(elapsed_window))) / elapsed_window
                total_elapsed = current_time - start_time
                logger.info(
                    "Strumieniowanie: łącznie wysłano %d zdarzeń (w tym %d anomalii, %.2f%%) w czasie %.1fs",
                    total_sent,
                    total_anomalies,
                    (total_anomalies / total_sent * 100) if total_sent > 0 else 0.0,
                    total_elapsed,
                )
                last_log_time = current_time

            # Precyzyjne zachowanie zadanej częstotliwości nadawania
            await asyncio.sleep(sleep_interval)

    except asyncio.CancelledError:
        logger.info("Zadanie asynchroniczne zostało anulowane.")
    finally:
        # Procedura bezpiecznego zamknięcia producenta (flush i stop)
        logger.info("Opróżnianie wewnętrznego bufora komunikatów aiokafka (flush)...")
        await producer.flush()
        logger.info("Zamykanie połączenia producenta z brokerem...")
        await producer.stop()

        total_duration = time.perf_counter() - start_time
        effective_rate = total_sent / total_duration if total_duration > 0 else 0.0

        logger.info("--- Podsumowanie sesji producenta Kafki ---")
        logger.info("Łączna liczba wysłanych zdarzeń:   %d", total_sent)
        logger.info("Liczba wykrytych anomalii:        %d", total_anomalies)
        logger.info("Odsetek anomalii:                 %.2f%%", (total_anomalies / total_sent * 100) if total_sent > 0 else 0.0)
        logger.info("Całkowity czas transmisji:         %.2f sekund", total_duration)
        logger.info("Średnia przepustowość:             %.2f zdarzeń/s", effective_rate)
        logger.info("Producent został bezpiecznie wyłączony.")


def main() -> None:
    config = parse_command_line_arguments()
    try:
        asyncio.run(run_event_producer(config))
    except KeyboardInterrupt:
        logger.info("Przerwano ręcznie przez użytkownika (KeyboardInterrupt).")


if __name__ == "__main__":
    main()
