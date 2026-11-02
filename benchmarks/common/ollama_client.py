"""
Klient Ollama dla scenariuszy benchmarkowych.

Każde wywołanie ustawia jawnie num_ctx, temperature, seed i num_predict (z konfiguracji) oraz
zwraca liczniki serwera: prompt_eval_count, eval_count, total_duration, load_duration,
prompt_eval_duration, eval_duration [ns]. Rekord jest oznaczany flagą `possible_truncation`, gdy
prompt_eval_count >= possible_truncation_ratio * num_ctx (Ollama obcina prompt do num_ctx).
W trybie strumieniowym mierzony jest czas do pierwszego fragmentu odpowiedzi (TTFT) oraz czas całkowity.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

import httpx

COUNTERS = ["prompt_eval_count", "eval_count", "total_duration", "load_duration", "prompt_eval_duration", "eval_duration"]


def build_options(cfg: Dict[str, Any], profile: str, num_ctx: Optional[int] = None,
                  num_predict: Optional[int] = None) -> Dict[str, Any]:
    llm = cfg["llm"]
    prof = llm["profiles"][profile]
    return {
        "num_ctx": int(num_ctx or llm["num_ctx"]),
        "num_predict": int(num_predict or prof["num_predict"]),
        "temperature": float(llm["temperature"]),
        "seed": int(cfg["seeds"]["llm"]),
        "stop": list(llm.get("stop", [])),
    }


async def generate(client: httpx.AsyncClient, base_url: str, model: str, prompt: str, options: Dict[str, Any],
                   stream: bool = False, truncation_ratio: float = 0.95) -> Dict[str, Any]:
    """Wywołuje /api/generate i zwraca tekst, liczniki serwera i czasy po stronie klienta."""
    payload = {"model": model, "prompt": prompt, "stream": stream, "options": options, "keep_alive": "30m"}
    t0 = time.perf_counter_ns()
    ttft_ns: Optional[int] = None
    text_parts: List[str] = []
    final: Dict[str, Any] = {}
    n_chunks = 0
    if stream:
        async with client.stream("POST", f"{base_url}/api/generate", json=payload) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line:
                    continue
                if ttft_ns is None:
                    ttft_ns = time.perf_counter_ns() - t0
                obj = json.loads(line)
                n_chunks += 1
                text_parts.append(obj.get("response", ""))
                if obj.get("done"):
                    final = obj
    else:
        resp = await client.post(f"{base_url}/api/generate", json=payload)
        resp.raise_for_status()
        final = resp.json()
        text_parts.append(final.get("response", ""))
    wall_ns = time.perf_counter_ns() - t0
    rec: Dict[str, Any] = {k: final.get(k) for k in COUNTERS}
    rec.update({
        "model": model,
        "response_text": "".join(text_parts),
        "done_reason": final.get("done_reason"),
        "client_wall_ms": wall_ns / 1e6,
        "client_ttft_ms": (ttft_ns / 1e6) if ttft_ns is not None else None,
        "stream_chunks": n_chunks if stream else None,
        "num_ctx": options.get("num_ctx"),
        "num_predict": options.get("num_predict"),
        "seed": options.get("seed"),
        "temperature": options.get("temperature"),
    })
    pec = rec.get("prompt_eval_count") or 0
    rec["possible_truncation"] = bool(pec >= truncation_ratio * int(options.get("num_ctx") or 0))
    return rec


async def model_digest(base_url: str, model: str) -> Optional[str]:
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(f"{base_url}/api/tags")
        for m in r.json().get("models", []):
            if m.get("name") == model or m.get("model") == model:
                return m.get("digest")
    return None


async def require_models(base_url: str, models: List[str]) -> Dict[str, str]:
    """Sprawdza przez /api/show, że modele istnieją, i zwraca ich digesty; brak modelu przerywa przebieg."""
    out: Dict[str, str] = {}
    async with httpx.AsyncClient(timeout=60) as c:
        for m in models:
            r = await c.post(f"{base_url}/api/show", json={"model": m})
            if r.status_code != 200:
                raise RuntimeError(f"Model {m} niedostępny w Ollama ({r.status_code}): {r.text[:200]}")
    for m in models:
        d = await model_digest(base_url, m)
        if not d:
            raise RuntimeError(f"Brak digestu modelu {m}")
        out[m] = d
    return out
