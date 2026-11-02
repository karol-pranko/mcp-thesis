#!/usr/bin/env python3
"""
Minimalni klienci MCP do benchmarków: stdio (serwer jako podproces) i SSE (HTTP).
Celowo bez SDK MCP - chodzi o to, żeby mierzyć sam transport, bez dodatkowych warstw.
"""

import asyncio
from dataclasses import dataclass, field
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Union

import httpx

logger = logging.getLogger("mcp_benchmark_client")


@dataclass
class ToolCallTiming:
    tool_name: str
    transport: str
    request_bytes: int
    response_bytes: int
    ttft_ms: float = 0.0         # czas do pierwszego bajtu odpowiedzi
    e2e_latency_ms: float = 0.0  # od wysłania do odebrania całej odpowiedzi
    success: bool = True
    error_message: Optional[str] = None


class StdioMcpClient:
    """JSON-RPC po jednej linii na stdin/stdout procesu serwera."""

    def __init__(self, server_command: Optional[List[str]] = None) -> None:
        self.server_command = server_command or [
            sys.executable,
            "-m",
            "src.mcp_server.server",
        ]
        self.process: Optional[asyncio.subprocess.Process] = None
        self._request_counter = 0

    async def start(self) -> None:
        logger.info("Uruchamianie procesu serwera MCP stdio: %s", " ".join(self.server_command))
        self.process = await asyncio.create_subprocess_exec(
            *self.server_command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    async def call_tool(
        self,
        tool_name: str,
        arguments: Dict[str, Any],
    ) -> Tuple[Any, ToolCallTiming]:
        """
        Zakłada, że odpowiedź przychodzi w jednej linii i nic innego nie pisze na stdout
        (logi serwera idą na stderr). Przy stdio TTFT == E2E, bo czytamy całą linię naraz.
        """
        if self.process is None or self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError("Proces serwera MCP stdio nie został uruchomiony.")

        self._request_counter += 1
        req_id = f"stdio_req_{self._request_counter}"
        rpc_request = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments,
            },
        }

        req_payload = json.dumps(rpc_request) + "\n"
        req_bytes = req_payload.encode("utf-8")

        t_start_ns = time.perf_counter_ns()
        self.process.stdin.write(req_bytes)
        await self.process.stdin.drain()

        line = await self.process.stdout.readline()
        t_end_ns = time.perf_counter_ns()

        e2e_ms = (t_end_ns - t_start_ns) / 1_000_000.0
        resp_len = len(line)

        try:
            response_json = json.loads(line.decode("utf-8"))
            result = response_json.get("result", {})
            timing = ToolCallTiming(
                tool_name=tool_name,
                transport="stdio",
                request_bytes=len(req_bytes),
                response_bytes=resp_len,
                ttft_ms=e2e_ms,
                e2e_latency_ms=e2e_ms,
                success=True,
            )
            return result, timing
        except Exception as exc:
            timing = ToolCallTiming(
                tool_name=tool_name,
                transport="stdio",
                request_bytes=len(req_bytes),
                response_bytes=resp_len,
                e2e_latency_ms=e2e_ms,
                success=False,
                error_message=str(exc),
            )
            raise

    async def close(self) -> None:
        # najpierw grzecznie, po 3 s kill
        if self.process is not None:
            try:
                self.process.terminate()
                await asyncio.wait_for(self.process.wait(), timeout=3.0)
            except Exception:
                self.process.kill()
            self.process = None


class SseMcpClient:
    """POST na /messages i czytanie odpowiedzi strumieniem. TTFT = pierwszy niepusty chunk."""

    def __init__(self, base_url: str = "http://127.0.0.1:8000") -> None:
        self.base_url = base_url.rstrip("/")
        self.client: Optional[httpx.AsyncClient] = None
        self._request_counter = 0

    async def start(self) -> None:
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)

    async def call_tool(
        self,
        tool_name: str,
        arguments: Dict[str, Any],
    ) -> Tuple[Any, ToolCallTiming]:
        if self.client is None:
            raise RuntimeError("Klient HTTP SSE nie został zainicjalizowany.")

        self._request_counter += 1
        req_id = f"sse_req_{self._request_counter}"
        rpc_request = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }

        req_payload = json.dumps(rpc_request)
        req_bytes = req_payload.encode("utf-8")

        t_start_ns = time.perf_counter_ns()
        ttft_ms = 0.0

        headers = {
            "Content-Type": "application/json",
            "Accept": "text/event-stream, application/json",
        }

        async with self.client.stream("POST", "/messages", content=req_bytes, headers=headers) as response:
            received_bytes = bytearray()
            async for chunk in response.aiter_bytes():
                if ttft_ms == 0.0 and len(chunk) > 0:
                    t_ttft_ns = time.perf_counter_ns()
                    ttft_ms = (t_ttft_ns - t_start_ns) / 1_000_000.0
                received_bytes.extend(chunk)

            t_end_ns = time.perf_counter_ns()
            e2e_ms = (t_end_ns - t_start_ns) / 1_000_000.0

            timing = ToolCallTiming(
                tool_name=tool_name,
                transport="SSE",
                request_bytes=len(req_bytes),
                response_bytes=len(received_bytes),
                ttft_ms=ttft_ms if ttft_ms > 0 else e2e_ms,
                e2e_latency_ms=e2e_ms,
                success=response.status_code == 200,
            )

            try:
                data = json.loads(received_bytes.decode("utf-8"))
                return data.get("result", data), timing
            except Exception:
                # surowe ramki SSE (data: ...) nie są JSON-em - oddajemy tekst jak jest
                return received_bytes.decode("utf-8", errors="replace"), timing

    async def close(self) -> None:
        if self.client is not None:
            await self.client.aclose()
            self.client = None
