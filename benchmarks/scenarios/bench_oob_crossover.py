#!/usr/bin/env python3
"""
Scenariusz B1: transfer In-Band (Base64 w JSON-RPC) a Out-of-Band (POSIX SHM, blob HTTP).

Tryby pomiaru (pole measurement_mode w każdym rekordzie):
  measured_loopback - profil loopback, bez żadnego sztucznego opóźnienia: mierzony jest rzeczywisty
                      koszt kodowania/dekodowania, SHA-256, alokacji i kopiowania SHM oraz zapisu/odczytu blobu;
  netem             - profile LAN/WAN: rzeczywisty transfer TCP między procesem serwera i klienta
                      przez interfejs lo z kolejką `tc qdisc netem delay <RTT/2> rate <B>` (wymaga NET_ADMIN i tc);
  emulated          - profile LAN/WAN bez NET_ADMIN: czas sieci dodawany funkcją inject_network_delay
                      według wzoru RTT + rozmiar*8/przepustowość; parametry wzoru są zapisywane w rekordzie
                      (emu_rtt_ms, emu_bandwidth_mbps, emulated_network_ms).

Składowe czasu (kolumny *_ns) pochodzą z instrumentacji src/mcp_server/out_of_band.TIMING_SINK
(base64_encode_ns, base64_decode_ns, sha256_ns, sha256_verify_ns, shm_alloc_ns, shm_copy_ns,
shm_read_ns, blob_write_ns, blob_read_ns) oraz z pomiaru transferu (transfer_ns).

S*: dla każdej pary (profil, tryb OOB) względem In-Band dopasowywane są proste L = a + b*S (MNK na
wszystkich powtórzeniach) dla `crossover_fit_neighbours` rozmiarów po każdej stronie zmiany znaku
różnicy median; S* = przecięcie prostych; 95% CI metodą bootstrapu (losowanie powtórzeń w obrębie
każdego rozmiaru, ziarno seeds.bootstrap).

H2 (profil loopback): In-Band vs SHM dla rozmiarów h2_sizes_bytes, co najmniej h2_min_repetitions
powtórzeń; test Manna-Whitneya i bootstrapowy CI różnicy median.

Pliki: oob_crossover_results.parquet/.csv, oob_crossover_summary.csv, oob_crossover_metrics.json,
oob_model_fit.json, oob_h2_tests.json.
"""
from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import os
import random
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.common.config import add_common_args, load_config  # noqa: E402
from benchmarks.common.provenance import capabilities, record_scenario_note  # noqa: E402
from benchmarks.common.stats import bootstrap_median_diff_ci, mann_whitney  # noqa: E402
import src.mcp_server.out_of_band as oob  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", stream=sys.stdout)
logger = logging.getLogger("bench_oob_crossover")

MODES = ["IN_BAND_BASE64", "OOB_POSIX_SHM", "OOB_HTTP_STREAM"]


def make_payload(size: int) -> bytes:
    pattern = b'{"id":1001,"status":"COMPLETED","metric":123.456,"desc":"MCP payload benchmark block"}\n'
    return (pattern * (size // len(pattern) + 1))[:size]


def inject_network_delay(delay_ms: float) -> float:
    """Opóźnienie emulowane (tryb `emulated`): sleep + aktywne oczekiwanie."""
    if delay_ms <= 0:
        return 0.0
    t0 = time.perf_counter()
    target = delay_ms / 1000.0
    if target > 0.0005:
        time.sleep(target - 0.0002)
    while time.perf_counter() - t0 < target:
        pass
    return (time.perf_counter() - t0) * 1000.0


# ---------------------------------------------------------------------------
# Pomiary lokalne (measured_loopback i emulated)
# ---------------------------------------------------------------------------
def _with_timings(fn, *a, **k) -> Tuple[Any, Dict[str, int]]:
    oob.TIMING_SINK = {}
    try:
        res = fn(*a, **k)
        return res, dict(oob.TIMING_SINK)
    finally:
        oob.TIMING_SINK = None


def measure_local(mode: str, data: bytes, storage_dir: Path, emu: Optional[Dict[str, float]], rnd: random.Random) -> Dict[str, Any]:
    """Jedno powtórzenie transferu; przy emu != None dodawany jest emulowany czas sieci."""
    oob.TIMING_SINK = {}
    emulated_ms = 0.0
    frame = ""
    t0 = time.perf_counter_ns()
    try:
        if mode == "IN_BAND_BASE64":
            desc, _ = oob.write_in_band(data)
            frame = json.dumps({"jsonrpc": "2.0", "result": desc, "id": 1})
            if emu:
                jit = rnd.uniform(-emu["jitter_ms"], emu["jitter_ms"]) if emu["jitter_ms"] else 0.0
                emulated_ms += inject_network_delay(max(0.001, emu["rtt_ms"] + jit) + len(frame) * 8 / (emu["bandwidth_mbps"] * 1e6) * 1000)
            got = oob.data_dispatcher.retrieve(json.loads(frame)["result"])
        elif mode == "OOB_POSIX_SHM":
            desc, _ = oob.write_posix_shm(data)
            frame = json.dumps({"jsonrpc": "2.0", "result": desc, "id": 1})
            got = oob.read_posix_shm(json.loads(frame)["result"]["name"], unlink_after_read=True)
        elif mode == "OOB_HTTP_STREAM":
            desc, _ = oob.write_http_stream_blob(data, storage_dir=storage_dir)
            frame = json.dumps({"jsonrpc": "2.0", "result": desc, "id": 1})
            if emu:
                for size in (len(frame), len(data)):
                    jit = rnd.uniform(-emu["jitter_ms"], emu["jitter_ms"]) if emu["jitter_ms"] else 0.0
                    emulated_ms += inject_network_delay(max(0.001, emu["rtt_ms"] + jit) + size * 8 / (emu["bandwidth_mbps"] * 1e6) * 1000)
            d = json.loads(frame)["result"]
            got = oob.data_dispatcher.retrieve(d)
            os.remove(d["file_path"])
        else:
            raise ValueError(mode)
        e2e = time.perf_counter_ns() - t0
        timings = dict(oob.TIMING_SINK)
    finally:
        oob.TIMING_SINK = None
    return {"e2e_ns": e2e, "sha256_verified": got == data, "frame_bytes": len(frame.encode()),
            "emulated_network_ms": emulated_ms if emu else None,
            "transfer_ns": int(emulated_ms * 1e6) if emu else None, **timings}


# ---------------------------------------------------------------------------
# Tryb netem: rzeczywisty transfer TCP przez lo z kolejką netem
# ---------------------------------------------------------------------------
def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(1 << 20, n - len(buf)))
        if not chunk:
            raise ConnectionError("połączenie zamknięte")
        buf += chunk
    return bytes(buf)


def _send_msg(sock: socket.socket, payload: bytes) -> None:
    sock.sendall(struct.pack("!Q", len(payload)) + payload)


def _recv_msg(sock: socket.socket) -> bytes:
    (n,) = struct.unpack("!Q", _recv_exact(sock, 8))
    return _recv_exact(sock, n)


def tcp_server(port: int, storage_dir: str, ready: Any) -> None:
    """Serwer bramy: generuje ładunek, przygotowuje ramkę In-Band lub blob OOB i odsyła przez TCP."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(8)
    ready.set()
    blobs: Dict[str, str] = {}
    while True:
        conn, _ = srv.accept()
        with conn:
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            req = json.loads(_recv_msg(conn))
            if req["op"] == "stop":
                _send_msg(conn, b"ok")
                return
            oob.TIMING_SINK = {}
            if req["op"] == "in_band":
                desc, _ = oob.write_in_band(make_payload(req["size"]))
                body = json.dumps({"jsonrpc": "2.0", "result": desc, "id": 1, "server_timings": oob.TIMING_SINK}).encode()
                _send_msg(conn, body)
            elif req["op"] == "oob_descriptor":
                desc, _ = oob.write_http_stream_blob(make_payload(req["size"]), storage_dir=Path(storage_dir))
                blobs[desc["blob_id"]] = desc["file_path"]
                _send_msg(conn, json.dumps({"jsonrpc": "2.0", "result": desc, "id": 1, "server_timings": oob.TIMING_SINK}).encode())
            elif req["op"] == "blob":
                path = blobs.pop(req["blob_id"])
                with open(path, "rb") as f:
                    _send_msg(conn, f.read())
                os.remove(path)
            oob.TIMING_SINK = None


def _request(port: int, req: Dict[str, Any]) -> bytes:
    with socket.create_connection(("127.0.0.1", port)) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        _send_msg(s, json.dumps(req).encode())
        return _recv_msg(s)


def measure_tcp(mode: str, size: int, port: int) -> Dict[str, Any]:
    """Jedno powtórzenie przez TCP (tryb netem); dekodowanie i weryfikacja SHA-256 po stronie klienta."""
    expected = make_payload(size)
    t0 = time.perf_counter_ns()
    if mode == "IN_BAND_BASE64":
        raw = _request(port, {"op": "in_band", "size": size})
        t_recv = time.perf_counter_ns()
        msg = json.loads(raw)
        got, client_t = _with_timings(oob.data_dispatcher.retrieve, msg["result"])
        server_t = msg["server_timings"]
    elif mode == "OOB_HTTP_STREAM":
        msg = json.loads(_request(port, {"op": "oob_descriptor", "size": size}))
        got = _request(port, {"op": "blob", "blob_id": msg["result"]["blob_id"]})
        t_recv = time.perf_counter_ns()
        import hashlib

        th = time.perf_counter_ns()
        ok = hashlib.sha256(got).hexdigest() == msg["result"]["sha256"]
        client_t = {"sha256_verify_ns": time.perf_counter_ns() - th}
        server_t = msg["server_timings"]
        if not ok:
            got = b""
    else:
        raise ValueError(mode)
    e2e = time.perf_counter_ns() - t0
    rec = {"e2e_ns": e2e, "transfer_ns": t_recv - t0, "sha256_verified": got == expected}
    rec.update({f"server_{k}": v for k, v in server_t.items()})
    rec.update(client_t)
    return rec


class Netem:
    def __init__(self, rtt_ms: float, bandwidth_mbps: float, jitter_ms: float) -> None:
        self.args = ["delay", f"{rtt_ms / 2:.3f}ms"]
        if jitter_ms:
            self.args += [f"{jitter_ms / 2:.3f}ms"]
        self.args += ["rate", f"{bandwidth_mbps:g}mbit"]

    def __enter__(self) -> "Netem":
        subprocess.run(["tc", "qdisc", "replace", "dev", "lo", "root", "netem", *self.args], check=True)
        return self

    def __exit__(self, *exc: Any) -> None:
        subprocess.run(["tc", "qdisc", "del", "dev", "lo", "root"], check=False)


def resolve_network_mode(requested: str) -> Tuple[str, str]:
    caps = capabilities()
    if requested == "emulated":
        return "emulated", "wymuszono --network-mode emulated"
    if caps["netem_possible"]:
        return "netem", "NET_ADMIN i tc dostępne"
    if requested == "netem":
        raise SystemExit("Zażądano --network-mode netem, ale brak NET_ADMIN lub tc.")
    return "emulated", f"netem niedostępny (tc={caps['tc']}, net_admin={caps['net_admin']}); użyto inject_network_delay"


# ---------------------------------------------------------------------------
# Analiza: S*, dopasowanie modelu, H2
# ---------------------------------------------------------------------------
def fit_line(sizes: np.ndarray, lat: np.ndarray) -> Tuple[float, float]:
    """MNK L = a + b*S; S skalowane do MB dla stabilności numerycznej, b zwracane w ms/bajt."""
    x = sizes.astype(float) / 1e6
    if len(np.unique(x)) < 2:
        return float(np.mean(lat)), float("nan")
    b_mb, a = np.polyfit(x, lat.astype(float), 1)
    return float(a), float(b_mb) / 1e6


def crossover(df: pd.DataFrame, oob_mode: str, neighbours: int, n_boot: int, seed: int) -> Dict[str, Any]:
    """S* = przecięcie prostych L = a + bS dopasowanych w otoczeniu zmiany znaku różnicy median."""
    med = df.groupby(["transport_mode", "payload_size_bytes"])["e2e_ms"].median().unstack(0)
    if "IN_BAND_BASE64" not in med or oob_mode not in med:
        return {"reported": False, "reason": "brak danych"}
    diff = (med["IN_BAND_BASE64"] - med[oob_mode]).dropna()
    sizes = diff.index.values
    sign_change = [i for i in range(len(diff) - 1) if diff.iloc[i] < 0 <= diff.iloc[i + 1]]
    if not sign_change:
        state = "OOB szybszy w całym zakresie" if (diff >= 0).all() else ("In-Band szybszy w całym zakresie" if (diff < 0).all() else "brak jednoznacznej zmiany znaku")
        return {"reported": False, "reason": state, "sizes": sizes.tolist()}
    i = sign_change[0]
    window = sizes[max(0, i - neighbours + 1): i + 1 + neighbours]
    sub = df[df["payload_size_bytes"].isin(window)]

    def est(d: pd.DataFrame) -> Optional[float]:
        a_in, b_in = fit_line(d[d.transport_mode == "IN_BAND_BASE64"].payload_size_bytes.values, d[d.transport_mode == "IN_BAND_BASE64"].e2e_ms.values)
        a_o, b_o = fit_line(d[d.transport_mode == oob_mode].payload_size_bytes.values, d[d.transport_mode == oob_mode].e2e_ms.values)
        if not np.isfinite(b_in) or not np.isfinite(b_o) or b_in == b_o:
            return None
        return (a_o - a_in) / (b_in - b_o)

    point = est(sub)
    rng = np.random.default_rng(seed)
    boots = []
    groups = {k: g for k, g in sub.groupby(["transport_mode", "payload_size_bytes"])}
    for _ in range(n_boot):
        parts = [g.iloc[rng.integers(0, len(g), len(g))] for g in groups.values()]
        v = est(pd.concat(parts))
        if v is not None and np.isfinite(v):
            boots.append(v)
    ci = (float(np.percentile(boots, 2.5)), float(np.percentile(boots, 97.5))) if len(boots) > 10 else (None, None)
    return {"reported": point is not None, "s_star_bytes": point, "s_star_kb": point / 1024 if point else None,
            "ci95_bytes": ci, "fit_window_bytes": window.tolist(), "bootstrap_resamples": n_boot, "seed": seed,
            "method": "przecięcie prostych L = a + b*S (MNK na powtórzeniach) w otoczeniu zmiany znaku różnicy median"}


def model_fit(df: pd.DataFrame) -> Dict[str, Any]:
    """Dopasowanie parametrów modelu kosztu L(S) = delta + S/przepustowość dla każdego trybu i profilu."""
    out: Dict[str, Any] = {}
    for (prof, mode), g in df.groupby(["network_profile", "transport_mode"]):
        a, b = fit_line(g.payload_size_bytes.values, g.e2e_ms.values)
        pred = a + b * g.payload_size_bytes.values
        ss_res = float(((g.e2e_ms.values - pred) ** 2).sum())
        ss_tot = float(((g.e2e_ms.values - g.e2e_ms.values.mean()) ** 2).sum()) or 1.0
        out.setdefault(prof, {})[mode] = {"delta_ms": a, "ms_per_byte": b, "throughput_mb_s": (1 / b / 1e3) if b > 0 else None,
                                          "r2": 1 - ss_res / ss_tot, "n": int(len(g))}
    return out


def main() -> None:
    ap = add_common_args(argparse.ArgumentParser(description=__doc__))
    ap.add_argument("--network-mode", choices=["auto", "netem", "emulated"], default=os.getenv("NETWORK_MODE", "auto"))
    args = ap.parse_args()
    cfg = load_config(args.config, args.mode)
    sc = cfg["scenarios"]["oob"]
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    net_mode, reason = resolve_network_mode(args.network_mode)
    if net_mode == "emulated":
        logger.warning("Profile LAN/WAN w trybie EMULATED: %s", reason)
    record_scenario_note(out, "oob", "network_mode", net_mode)
    record_scenario_note(out, "oob", "network_mode_reason", reason)
    record_scenario_note(out, "oob", "profiles", sc["profiles"])
    rnd = random.Random(cfg["seeds"]["bootstrap"])
    storage = Path(tempfile.mkdtemp(prefix="mcp_oob_bench_"))
    rows: List[Dict[str, Any]] = []
    try:
        for _ in range(sc["warmup"]):
            for mode in MODES:
                measure_local(mode, make_payload(65536), storage, None, rnd)
        # loopback: zawsze pomiar rzeczywisty, bez sztucznego opóźnienia
        for size in sc["payload_sizes_bytes"]:
            data = make_payload(size)
            reps = max(sc["repetitions"], sc["h2_min_repetitions"] if size in sc["h2_sizes_bytes"] else 0)
            for mode in MODES:
                for rep in range(1, reps + 1):
                    r = measure_local(mode, data, storage, None, rnd)
                    rows.append({"network_profile": "loopback", "measurement_mode": "measured_loopback", "transport_mode": mode,
                                 "payload_size_bytes": size, "repetition": rep, **r})
        # LAN/WAN: netem lub emulated
        for prof_name, prof in sc["profiles"].items():
            if net_mode == "netem":
                ctx = mp.get_context("spawn")
                port = 18900
                ready = ctx.Event()
                srv = ctx.Process(target=tcp_server, args=(port, str(storage), ready), daemon=True)
                srv.start()
                ready.wait(30)
                try:
                    with Netem(prof["rtt_ms"], prof["bandwidth_mbps"], prof["jitter_ms"]):
                        for size in sc["payload_sizes_bytes"]:
                            for mode in ("IN_BAND_BASE64", "OOB_HTTP_STREAM"):
                                for rep in range(1, sc["repetitions"] + 1):
                                    r = measure_tcp(mode, size, port)
                                    rows.append({"network_profile": prof_name, "measurement_mode": "netem", "transport_mode": mode,
                                                 "payload_size_bytes": size, "repetition": rep, "netem_rtt_ms": prof["rtt_ms"],
                                                 "netem_bandwidth_mbps": prof["bandwidth_mbps"], **r})
                finally:
                    try:
                        _request(port, {"op": "stop"})
                    except Exception:
                        pass
                    srv.join(5)
                    if srv.is_alive():
                        srv.terminate()
            else:
                for size in sc["payload_sizes_bytes"]:
                    data = make_payload(size)
                    for mode in ("IN_BAND_BASE64", "OOB_HTTP_STREAM"):
                        for rep in range(1, sc["repetitions"] + 1):
                            r = measure_local(mode, data, storage, prof, rnd)
                            rows.append({"network_profile": prof_name, "measurement_mode": "emulated", "transport_mode": mode,
                                         "payload_size_bytes": size, "repetition": rep, "emu_rtt_ms": prof["rtt_ms"],
                                         "emu_jitter_ms": prof["jitter_ms"], "emu_bandwidth_mbps": prof["bandwidth_mbps"], **r})
            logger.info("Profil %s (%s) gotowy", prof_name, net_mode)
    finally:
        shutil.rmtree(storage, ignore_errors=True)

    df = pd.DataFrame(rows)
    df["e2e_ms"] = df["e2e_ns"] / 1e6
    if not df["sha256_verified"].all():
        raise SystemExit("Błąd integralności SHA-256 w pomiarach OOB.")
    df.to_parquet(out / "oob_crossover_results.parquet", index=False)
    df.to_csv(out / "oob_crossover_results.csv", index=False)
    df.groupby(["network_profile", "measurement_mode", "transport_mode", "payload_size_bytes"]).agg(
        n=("e2e_ms", "count"), median_ms=("e2e_ms", "median"), mean_ms=("e2e_ms", "mean"),
        p95_ms=("e2e_ms", lambda x: np.percentile(x, 95)), std_ms=("e2e_ms", "std"),
    ).reset_index().to_csv(out / "oob_crossover_summary.csv", index=False)

    seed = cfg["seeds"]["bootstrap"]
    metrics: Dict[str, Any] = {"network_mode": net_mode, "network_mode_reason": reason, "profiles": {}}
    for prof, g in df.groupby("network_profile"):
        modes = [m for m in ("OOB_POSIX_SHM", "OOB_HTTP_STREAM") if m in set(g.transport_mode)]
        metrics["profiles"][prof] = {"measurement_mode": g.measurement_mode.iloc[0],
                                     **{m: crossover(g, m, sc["crossover_fit_neighbours"], sc["bootstrap_resamples"], seed) for m in modes}}
    (out / "oob_crossover_metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    (out / "oob_model_fit.json").write_text(json.dumps(model_fit(df), ensure_ascii=False, indent=2), encoding="utf-8")

    h2 = []
    lb = df[df.network_profile == "loopback"]
    for size in sc["h2_sizes_bytes"]:
        a = lb[(lb.payload_size_bytes == size) & (lb.transport_mode == "OOB_POSIX_SHM")]["e2e_ms"]
        b = lb[(lb.payload_size_bytes == size) & (lb.transport_mode == "IN_BAND_BASE64")]["e2e_ms"]
        h2.append({"hypothesis": "H2", "payload_size_bytes": size, "comparison": "OOB_POSIX_SHM vs IN_BAND_BASE64 (loopback)",
                   **mann_whitney(a, b), "median_diff_ci": bootstrap_median_diff_ci(a, b, seed, sc["bootstrap_resamples"])})
    (out / "oob_h2_tests.json").write_text(json.dumps(h2, ensure_ascii=False, indent=2, default=float), encoding="utf-8")


if __name__ == "__main__":
    main()
