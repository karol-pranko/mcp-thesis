#!/usr/bin/env python3
"""Bufor anomalii w konsumencie Kafki + zasoby/narzędzie MCP, które go czytają. Bez brokera."""

from pathlib import Path
import sys

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import json
import pytest
from src.mcp_server.streaming import KafkaTelemetryConsumer, streaming_consumer
from src.mcp_server.server import mcp


class TestKafkaTelemetryConsumer:
    def test_consumer_initial_state(self):
        consumer = KafkaTelemetryConsumer(buffer_size=50)
        assert consumer.running is False
        assert consumer.total_events_received == 0
        assert consumer.total_anomalies_detected == 0
        assert consumer.get_latest_anomaly() is None
        assert consumer.get_latest_anomalies() == []

        empty_json = consumer.get_latest_anomaly_json()
        parsed = json.loads(empty_json)
        assert parsed["status"] == "NO_ANOMALY_RECORDED"

    def test_buffer_and_latest_anomaly_ordering(self):
        consumer = KafkaTelemetryConsumer(buffer_size=5)

        sample_anomalies = [
            {"event_id": f"evt-{i}", "device_id": f"DEV_{i}", "status": "CRITICAL_ANOMALY", "temperature": 120 + i}
            for i in range(1, 8)
        ]

        for anomaly in sample_anomalies:
            consumer.latest_anomaly = anomaly
            consumer.anomalies_buffer.append(anomaly)
            consumer.total_anomalies_detected += 1
            consumer.total_events_received += 1

        # bufor 5 -> zostają evt-3..evt-7
        assert len(consumer.anomalies_buffer) == 5
        assert consumer.total_events_received == 7
        assert consumer.total_anomalies_detected == 7

        assert consumer.get_latest_anomaly()["event_id"] == "evt-7"

        top_3 = consumer.get_latest_anomalies(limit=3)
        assert len(top_3) == 3
        assert top_3[0]["event_id"] == "evt-7"
        assert top_3[1]["event_id"] == "evt-6"
        assert top_3[2]["event_id"] == "evt-5"

    def test_get_latest_anomaly_json_format(self):
        consumer = KafkaTelemetryConsumer()
        anomaly_data = {
            "event_id": "test-uuid-1234",
            "device_id": "DEV_SENSOR_042",
            "temperature": 145.8,
            "pressure": 1250.0,
            "status": "CRITICAL_ANOMALY",
        }
        consumer.latest_anomaly = anomaly_data

        raw_json = consumer.get_latest_anomaly_json()
        parsed = json.loads(raw_json)
        assert parsed["event_id"] == "test-uuid-1234"
        assert parsed["device_id"] == "DEV_SENSOR_042"
        assert parsed["status"] == "CRITICAL_ANOMALY"


@pytest.mark.asyncio
async def test_mcp_integration_telemetry_resource_and_tool():
    test_anomaly = {
        "event_id": "integration-uuid-999",
        "device_id": "DEV_SENSOR_099",
        "temperature": 152.3,
        "pressure": 1340.2,
        "status": "CRITICAL_ANOMALY",
    }
    # wstrzykujemy anomalię prosto do globalnego konsumenta, bez Kafki
    streaming_consumer.latest_anomaly = test_anomaly
    streaming_consumer.anomalies_buffer.append(test_anomaly)

    resources = await mcp.list_resources()
    uris = [str(r.uri) for r in resources]
    assert "telemetry://latest_anomaly" in uris
    assert "telemetry://live_stream" in uris

    resource_content = await mcp.read_resource("telemetry://latest_anomaly")
    assert len(resource_content.contents) > 0
    parsed_resource = json.loads(resource_content.contents[0].content)
    assert parsed_resource["event_id"] == "integration-uuid-999"
    assert parsed_resource["device_id"] == "DEV_SENSOR_099"

    live_content = await mcp.read_resource("telemetry://live_stream")
    assert len(live_content.contents) > 0
    parsed_live = json.loads(live_content.contents[0].content)
    assert isinstance(parsed_live, list)
    assert len(parsed_live) >= 1
    assert parsed_live[0]["event_id"] == "integration-uuid-999"

    tool_result = await mcp.call_tool("get_latest_telemetry_anomalies", {"limit": 1})
    raw_json = tool_result.structured_content["result"]
    anomalies_list = json.loads(raw_json)
    assert len(anomalies_list) == 1
    assert anomalies_list[0]["event_id"] == "integration-uuid-999"
