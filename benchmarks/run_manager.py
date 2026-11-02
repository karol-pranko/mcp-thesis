#!/usr/bin/env python3
"""
Zarządzanie przebiegiem pomiarowym wywoływane przez run_benchmark.sh.

Podpolecenia:
  check-empty            kod 3, jeśli katalog wyników zawiera pliki (poza .gitkeep)
  archive                przenosi zawartość katalogu wyników do raw_results_archive/<znacznik>/
  init                   tworzy manifest.json (run_id, git, konfiguracja, ziarna, tryb) i environment.json
  scenario-start NAME    zapisuje czas rozpoczęcia scenariusza
  scenario-end NAME RC   zapisuje czas zakończenia, kod wyjścia i sumy SHA-256 plików
  is-done NAME           kod 0, jeśli scenariusz ma w manifeście status "completed"
  finalize               czas zakończenia rundy i sumy SHA-256
  estimate               szacowany czas scenariuszy na podstawie konfiguracji
  preflight              kontrole wstępne (usługi, dane, modele, GPU, dysk, sieć)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.common import provenance as prov  # noqa: E402
from benchmarks.common.config import ConfigError, db_params, load_config  # noqa: E402

SCENARIO_ORDER = [
    "data", "pagination", "oob", "concurrency", "streaming", "security", "drift",
    "ablation", "ttft_scaling", "schema_quality", "statistics", "plots", "validation",
]


def cmd_check_empty(args: argparse.Namespace) -> int:
    d = Path(args.output_dir)
    items = [p for p in d.iterdir() if p.name != ".gitkeep"] if d.exists() else []
    if items:
        print(f"Katalog wyników {d} zawiera {len(items)} element(ów).")
        return 3
    return 0


def cmd_archive(args: argparse.Namespace) -> int:
    dest = prov.archive_existing(Path(args.output_dir))
    print(f"Zarchiwizowano poprzednie wyniki do: {dest}" if dest else "Brak wyników do archiwizacji.")
    return 0


def cmd_init(args: argparse.Namespace) -> int:
    cfg = load_config(args.config, args.mode)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "logs").mkdir(exist_ok=True)
    existing = prov.read_manifest(out)
    if existing and args.resume:
        prov.update_manifest(out, {"resumed_at": existing.get("resumed_at", []) + [prov.now_iso()], "git_at_resume": prov.git_info()})
        print(f"Wznowienie przebiegu {existing.get('run_id')}")
        return 0
    run_id = args.run_id or f"{prov.now_iso()[:19].replace(':', '').replace('-', '')}_{uuid.uuid4().hex[:6]}"
    git = prov.git_info()
    manifest = {
        "run_id": run_id,
        "created_at": prov.now_iso(),
        "mode": args.mode,
        "git": git,
        "config": cfg,
        "seeds": cfg["seeds"],
        "smoke_test_model_substitution": cfg["llm"]["smoke_test_model_substitution"],
        "network_mode_requested": args.network_mode,
        "scenario_order": SCENARIO_ORDER,
        "scenarios": {},
        "scenario_notes": {},
        "pinned_service_versions": _pinned_versions(),
    }
    prov.update_manifest(out, manifest)
    env = prov.capture_environment(out, cfg, db_params(cfg))
    prov.update_manifest(out, {"service_versions_observed": {
        "postgres": env.get("postgres", {}).get("version"),
        "extensions": env.get("postgres", {}).get("extensions"),
        "kafka": env.get("kafka"),
        "ollama": env.get("ollama", {}).get("version"),
    }, "capabilities": env.get("capabilities")})
    print(run_id)
    return 0


def _pinned_versions() -> Dict[str, Any]:
    """Wersje usług przypięte w scripts/versions.env (źródło dla setup_runpod.sh i docker-compose)."""
    p = PROJECT_ROOT / "scripts" / "versions.env"
    out: Dict[str, Any] = {}
    if p.exists():
        for line in p.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                out[k] = v.strip('"')
    return out


def cmd_scenario_start(args: argparse.Namespace) -> int:
    prov.update_manifest(Path(args.output_dir), {"scenarios": {args.name: {
        "status": "running", "started_at": prov.now_iso(), "log": f"logs/{args.name}.log"}}})
    return 0


def cmd_scenario_end(args: argparse.Namespace) -> int:
    out = Path(args.output_dir)
    m = prov.read_manifest(out)
    entry = dict(m.get("scenarios", {}).get(args.name, {}))
    entry.update({"finished_at": prov.now_iso(), "exit_code": int(args.rc),
                  "status": "completed" if int(args.rc) == 0 else "failed"})
    prov.update_manifest(out, {"scenarios": {args.name: entry}, "files_sha256": prov.file_hashes(out)})
    return 0


def cmd_is_done(args: argparse.Namespace) -> int:
    m = prov.read_manifest(Path(args.output_dir))
    return 0 if m.get("scenarios", {}).get(args.name, {}).get("status") == "completed" else 1


def cmd_finalize(args: argparse.Namespace) -> int:
    out = Path(args.output_dir)
    prov.update_manifest(out, {"finished_at": prov.now_iso(), "final_status": args.status,
                               "files_sha256": prov.file_hashes(out)})
    return 0


# ---------------------------------------------------------------------------
# Szacowanie czasu
# ---------------------------------------------------------------------------
def estimate(cfg: Dict[str, Any]) -> Dict[str, float]:
    """
    Szacowany czas [s] scenariuszy wyliczany z konfiguracji. Stałe kosztów jednostkowych pochodzą
    z poprzednich przebiegów (backup2/benchmarks_results_full.tar.gz, pliki raw_results z 2026-10-03)
    i są przybliżeniem; rzeczywisty czas zależy od GPU.
    """
    s = cfg["scenarios"]
    est: Dict[str, float] = {}
    rows = cfg["data"]["rows_per_table"]
    est["data"] = 60 + 1000 * rows * 0.0004
    p = s["pagination"]
    per = 0.0
    for off in p["offsets"]:
        per += 0.0005                      # keyset z indeksem ~0,5 ms
        per += 0.00045 * off / 1000.0      # OFFSET/LIMIT ~0,45 ms na 1000 pominiętych wierszy
        per += 0.2 if off <= p["unindexed_max_offset"] else 0.0  # skan sekwencyjny ~200 ms
    # dwie postacie predykatu (DNF, UNION ALL) + warianty bazowe, spójność i EXPLAIN
    est["pagination"] = 30 + (p["repetitions"] + p["warmup"]) * per * 2 + 10 * len(p["explain_offsets"])
    o = s["oob"]
    sizes = o["payload_sizes_bytes"]
    t = 0.0
    for size in sizes:
        inband_cpu = size / 78e6  # ~78 MB/s In-Band (Base64 + JSON + SHA-256)
        shm = size / 600e6
        t += inband_cpu + shm  # loopback
        for prof in o["profiles"].values():
            wire = size * 8 / (prof["bandwidth_mbps"] * 1e6)
            t += inband_cpu * 1.33 + prof["rtt_ms"] / 1000 + wire * 1.33 + 2 * prof["rtt_ms"] / 1000 + wire
    est["oob"] = 20 + t * (o["repetitions"] + o["warmup"]) + 5
    c = s["concurrency"]
    lvl = c["level_duration_s"] + c["level_warmup_s"] + 2
    n_main = len(c["levels"]) * len(c["variants"])
    n_grid = sum(1 for _ in c["workers_grid"] if _ != c["default_workers"]) * 2
    est["concurrency"] = (n_main + n_grid) * lvl + 10
    st = s["streaming"]
    variants = 1 + len(st["poll_intervals_s"])
    est["streaming"] = variants * (st["idle_window_s"] + st["n_events"] * st["mean_interarrival_s"] + max(st["poll_intervals_s"]) + 20)
    # llm_injection: ok. 250 wektorów x 2 warianty x ok. 3 s
    est["security"] = 60 + (1500 if s["security"].get("llm_injection") else 0)
    est["drift"] = 60
    a = s["ablation"]
    est["ablation"] = 30 + len(a["m_exp_values"]) * a["n_queries"] * 0.03
    tt = s["ttft_scaling"]
    if tt.get("enabled", True):
        per_call = sum(n / 3000.0 + tt["num_predict"] / 40.0 + 1.0 for n in tt["ddl_token_targets"])
        est["ttft_scaling"] = len(tt["profiles"]) * (per_call * tt["repetitions"] + 60)
    else:
        est["ttft_scaling"] = 0
    sq = s["schema_quality"]
    n_gen = 5 + len(sq["equal_budget"]["strategies"])
    # ok. 2,1 s ściany na wywołanie przy współbieżności LLM 4-8 (przebieg 2026-10-03: 5000 wywołań w ok. 2,9 h)
    est["schema_quality"] = 120 + len(sq["profiles"]) * sq["n_queries"] * n_gen * 2.1
    est["statistics"] = 60
    est["plots"] = 60
    est["validation"] = 10
    return est


def cmd_estimate(args: argparse.Namespace) -> int:
    cfg = load_config(args.config, args.mode)
    est = estimate(cfg)
    total = 0.0
    print("Szacowany czas scenariuszy (z konfiguracji; przybliżenie):")
    for name in SCENARIO_ORDER:
        if name in est:
            total += est[name]
            print(f"  {name:16s} {est[name] / 60:8.1f} min")
    print(f"  {'RAZEM':16s} {total / 3600:8.2f} h")
    if args.output_dir and Path(args.output_dir, "manifest.json").exists():
        prov.update_manifest(Path(args.output_dir), {"estimated_duration_s": est})
    return 0


# ---------------------------------------------------------------------------
# Kontrole wstępne
# ---------------------------------------------------------------------------
class PreflightError(RuntimeError):
    pass


def _check_postgres(db: Dict[str, Any]) -> Dict[str, Any]:
    import asyncpg

    async def q() -> Dict[str, Any]:
        conn = await asyncpg.connect(**db, timeout=10)
        try:
            n_tables = await conn.fetchval("SELECT count(*) FROM public.schema_embeddings")
            n_fk = await conn.fetchval("SELECT count(*) FROM public.schema_foreign_keys")
            n_orders = await conn.fetchval("SELECT count(*) FROM public.analytics_orders")
            empty = await conn.fetch(
                "SELECT e.table_name FROM public.schema_embeddings e "
                "WHERE NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = e.table_name AND c.relkind = 'r')")
            return {"schema_tables": n_tables, "foreign_keys": n_fk, "analytics_orders": n_orders, "missing_tables": [r[0] for r in empty]}
        finally:
            await conn.close()

    return asyncio.run(q())


async def _empty_schema_tables(db: Dict[str, Any]) -> List[str]:
    """Zwraca listę tabel schematu (schema_embeddings) bez ani jednego wiersza."""
    import asyncpg

    conn = await asyncpg.connect(**db, timeout=10)
    try:
        names = [r[0] for r in await conn.fetch("SELECT table_name FROM public.schema_embeddings ORDER BY table_name")]
        empty = []
        for n in names:
            if not await conn.fetchval(f'SELECT EXISTS (SELECT 1 FROM public."{n}")'):
                empty.append(n)
        return empty
    finally:
        await conn.close()


def cmd_check_data(args: argparse.Namespace) -> int:
    """Kod 4, jeśli którakolwiek tabela schematu (z której losowane są ścieżki GT) jest pusta."""
    cfg = load_config(args.config, args.mode)
    empty = asyncio.run(_empty_schema_tables(db_params(cfg)))
    print(f"Puste tabele schematu: {len(empty)}")
    if args.output_dir and Path(args.output_dir, "manifest.json").exists():
        prov.record_scenario_note(Path(args.output_dir), "data", "empty_schema_tables", len(empty))
    return 4 if empty else 0


def cmd_preflight(args: argparse.Namespace) -> int:
    import httpx

    cfg = load_config(args.config, args.mode)
    db = db_params(cfg)
    problems: List[str] = []
    info: Dict[str, Any] = {}
    try:
        info["postgres"] = _check_postgres(db)
        if info["postgres"]["schema_tables"] < 1000 and args.mode == "full":
            problems.append(f"schema_embeddings ma {info['postgres']['schema_tables']} tabel (oczekiwano 1000)")
        if info["postgres"]["missing_tables"]:
            problems.append(f"brak {len(info['postgres']['missing_tables'])} tabel fizycznych schematu")
        if info["postgres"]["analytics_orders"] < 1_000_000 and args.mode == "full":
            problems.append("analytics_orders ma < 1 000 000 wierszy")
    except Exception as exc:
        problems.append(f"PostgreSQL niedostępny: {exc}")
    # Kafka
    import socket

    host, port = (cfg.get("kafka_bootstrap") or __import__("os").getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")).split(",")[0].split(":")
    try:
        socket.create_connection((host, int(port)), timeout=5).close()
        info["kafka"] = "ok"
    except Exception as exc:
        problems.append(f"Kafka niedostępna ({host}:{port}): {exc}")
    # Ollama i modele
    base = cfg["llm"]["base_url"]
    needed = sorted({cfg["llm"]["profiles"][p]["model"] for sc in ("schema_quality", "ttft_scaling")
                     for p in cfg["scenarios"][sc].get("profiles", [])})
    info["models"] = {}
    try:
        for model in needed:
            r = httpx.post(f"{base}/api/show", json={"model": model}, timeout=30)
            if r.status_code != 200:
                problems.append(f"model {model} niedostępny w Ollama ({r.status_code})")
                continue
            tags = httpx.get(f"{base}/api/tags", timeout=10).json().get("models", [])
            digest = next((t.get("digest") for t in tags if t.get("name") == model or t.get("model") == model), None)
            info["models"][model] = digest
            if not digest:
                problems.append(f"brak digestu modelu {model}")
    except Exception as exc:
        problems.append(f"Ollama niedostępna ({base}): {exc}")
    # GPU
    smi = prov.run_cmd(["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"])
    if smi.get("returncode") == 0 and smi["stdout"]:
        name, mem = [x.strip() for x in smi["stdout"].splitlines()[0].split(",")]
        info["gpu"] = {"name": name, "memory_mib": int(float(mem))}
        uses_large = any("large" in cfg["scenarios"][sc].get("profiles", []) for sc in ("schema_quality", "ttft_scaling"))
        if args.mode == "full" and uses_large and int(float(mem)) < cfg["llm"]["min_vram_mib_for_large"]:
            problems.append(f"GPU {name}: {mem} MiB VRAM < {cfg['llm']['min_vram_mib_for_large']} MiB wymaganych dla profilu large")
    else:
        msg = "nvidia-smi niedostępne (brak widocznego GPU)"
        (problems.append(msg) if args.mode == "full" else info.setdefault("warnings", []).append(msg))
    # Dysk
    free = shutil.disk_usage(Path(args.output_dir).resolve().anchor if not Path(args.output_dir).exists() else args.output_dir).free
    info["disk_free_gb"] = round(free / 1e9, 1)
    if args.mode == "full" and free < 20e9:
        problems.append(f"za mało miejsca na dysku: {free / 1e9:.1f} GB (< 20 GB)")
    info["capabilities"] = prov.capabilities()
    print(json.dumps(info, ensure_ascii=False, indent=2, default=str))
    if Path(args.output_dir, "manifest.json").exists():
        prov.update_manifest(Path(args.output_dir), {"preflight": {"info": info, "problems": problems, "at": prov.now_iso()},
                                                     "model_digests": info.get("models", {})})
    if problems:
        print("\nKONTROLA WSTĘPNA NIEUDANA:")
        for p in problems:
            print(f"  - {p}")
        return 5
    print("Kontrola wstępna: OK")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command")
    ap.add_argument("name", nargs="?")
    ap.add_argument("rc", nargs="?", default="0")
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--config", default=str(PROJECT_ROOT / "benchmarks" / "config" / "benchmark_config.json"))
    ap.add_argument("--mode", choices=["full", "quick"], default="full")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--network-mode", default="auto")
    ap.add_argument("--status", default="completed")
    args = ap.parse_args()
    handlers = {
        "check-empty": cmd_check_empty, "archive": cmd_archive, "init": cmd_init,
        "scenario-start": cmd_scenario_start, "scenario-end": cmd_scenario_end, "is-done": cmd_is_done,
        "finalize": cmd_finalize, "estimate": cmd_estimate, "preflight": cmd_preflight, "check-data": cmd_check_data,
    }
    if args.command not in handlers:
        ap.error(f"nieznane polecenie {args.command}")
    try:
        return handlers[args.command](args)
    except ConfigError as exc:
        print(f"Błąd konfiguracji: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
