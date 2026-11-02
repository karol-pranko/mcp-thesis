#!/usr/bin/env python3
"""
Audyt, Krok 2.3: próby odtworzenia zawartości podejrzanych plików kodem losującym z tym samym ziarnem.

1. raw_results/llm_inference_latencies.csv  <- generate_publication_plots.py l. 1121-1131 (seed 2026)
   oraz scripts/generate_schema_discovery_and_pagination_plots.py l. 250-267 (seed 42).
2. results-remote-gpu/streaming_comparison.json (źródło rys. 9): próba dopasowania próbek
   REST Polling do np.random.uniform(20, 990) / (50, 4950) oraz SSE do np.random.normal(1.34, 0.25)
   (fallback w scripts/generate_streaming_and_security_plots.py l. 86-104) dla ziaren 0..200 000,
   także w kolejności wywołań tego skryptu (normal -> uniform -> uniform).
3. Sprawdzenie, czy w plikach CSV raw_results występują wartości przycięte do granic np.clip
   (charakterystyczny ślad generatorów: wiele wartości równych dokładnie granicy).

Wynik: out/a03_odtwarzanie.txt
"""
import io
import json
from contextlib import redirect_stdout
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "benchmarks" / "raw_results"
OUT = Path(__file__).resolve().parent / "out"


def ttft() -> None:
    lat = pd.read_csv(RAW / "llm_inference_latencies.csv")
    m = lat[lat["approach"].str.startswith("MCP")].reset_index(drop=True)
    b = lat[~lat["approach"].str.startswith("MCP")].reset_index(drop=True)
    for seed, clip in [(2026, True), (42, True), (42, False)]:
        np.random.seed(seed)
        g = [np.random.normal(34.2, 3.6, 100), np.random.normal(345.0, 22.8, 100),
             np.random.normal(1380.0, 85.0, 100), np.random.normal(2420.0, 120.0, 100)]
        if clip:
            g = [np.clip(g[0], 25, 50), np.clip(g[1], 280, 420), np.clip(g[2], 1150, 1650), np.clip(g[3], 2100, 2800)]
        files = [m["ttft_ms"], m["e2e_latency_ms"], b["ttft_ms"], b["e2e_latency_ms"]]
        diffs = [float(np.abs(np.round(x, 3) - y.to_numpy()).max()) for x, y in zip(g, files)]
        print(f"  seed={seed} clip={clip}: max |różnica| per kolumna = {diffs}")
    print(f"  wartości MCP TTFT równe dolnej granicy clip 25.0: {(m['ttft_ms'] == 25.0).sum()}")


def streaming_search(max_seed: int = 200_000) -> None:
    data = {d["approach"]: np.array(d.get("raw_samples", []), dtype=float)
            for d in json.load(open(ROOT / "results-remote-gpu" / "streaming_comparison.json", encoding="utf-8"))}
    sse, p1, p5 = data["MCP_SSE_PUSH"], data["REST_POLLING_1S"], data["REST_POLLING_5S"]
    print(f"  n: SSE={len(sse)} P1={len(p1)} P5={len(p5)}; min/max P1=({p1.min():.1f}, {p1.max():.1f}), P5=({p5.min():.1f}, {p5.max():.1f})")
    hits = []
    for seed in range(max_seed):
        rs = np.random.RandomState(seed)
        a = rs.normal(1.34, 0.25, 3)
        if np.allclose(np.round(a, 3), sse[:3], atol=1e-3):
            hits.append(("SSE normal", seed))
        rs = np.random.RandomState(seed)
        u = rs.uniform(20.0, 990.0, 3)
        if np.allclose(np.round(u, 3), p1[:3], atol=1e-3):
            hits.append(("P1 uniform", seed))
        rs = np.random.RandomState(seed)
        u5 = rs.uniform(50.0, 4950.0, 3)
        if np.allclose(np.round(u5, 3), p5[:3], atol=1e-3):
            hits.append(("P5 uniform", seed))
        rs = np.random.RandomState(seed)  # kolejność wywołań w generate_streaming_and_security_plots.py
        rs.normal(1.34, 0.25, 100)
        u = rs.uniform(20.0, 990.0, 3)
        if np.allclose(np.round(u, 3), p1[:3], atol=1e-3):
            hits.append(("P1 po normal(100)", seed))
    print(f"  przeszukano ziarna 0..{max_seed - 1}; trafienia: {hits if hits else 'brak'}")
    # Zgodność z modelem fazowym: opóźnienie odpytywania ~ U(10 ms, interwał) + narzut
    for name, s, iv in [("P1", p1, 1000.0), ("P5", p5, 5000.0)]:
        frac_in = np.mean((s >= 10.0) & (s <= iv + 50.0))
        print(f"  {name}: odsetek próbek w [10 ms, interwał+50 ms] = {frac_in:.2f}; średnia = {s.mean():.1f} (U(10, {iv:.0f}) -> {(10 + iv) / 2:.0f})")


def clip_traces() -> None:
    for f in sorted(RAW.glob("*.csv")):
        try:
            df = pd.read_csv(f)
        except Exception:
            continue
        for c in df.select_dtypes("number").columns:
            s = df[c].dropna()
            if len(s) < 50 or s.nunique() < 10:
                continue
            for edge in (s.min(), s.max()):
                k = int((s == edge).sum())
                if k >= 3 and k / len(s) > 0.01 and not float(edge).is_integer():
                    print(f"  {f.name}:{c} - {k} wartości równych {edge} ({k / len(s):.1%})")


def main() -> None:
    buf = io.StringIO()
    with redirect_stdout(buf):
        print("=== 1. llm_inference_latencies.csv ===")
        ttft()
        print("\n=== 2. results-remote-gpu/streaming_comparison.json (rys. 9) ===")
        streaming_search()
        print("\n=== 3. Ślady przycinania np.clip w raw_results (>=3 wartości na niecałkowitej granicy) ===")
        clip_traces()
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "a03_odtwarzanie.txt").write_text(buf.getvalue(), encoding="utf-8")
    print(buf.getvalue())


if __name__ == "__main__":
    main()
