"""
Pochodzenie wyników: manifest.json i environment.json w katalogu wyników.

manifest.json zawiera identyfikator przebiegu, commit git i flagę dirty, pełną konfigurację,
ziarna, tryb, czasy rozpoczęcia i zakończenia każdego scenariusza oraz sumy SHA-256 wszystkich
plików wyników. Aktualizowany jest po każdym scenariuszu (zapis atomowy pod blokadą pliku).
"""
from __future__ import annotations

import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Zmienne RunPod zapisywane w environment.json (bez sekretów i tokenów)
RUNPOD_SAFE_VARS = [
    "RUNPOD_POD_ID", "RUNPOD_GPU_COUNT", "RUNPOD_CPU_COUNT", "RUNPOD_MEM_GB", "RUNPOD_POD_HOSTNAME",
    "RUNPOD_DC_ID", "RUNPOD_VOLUME_ID", "RUNPOD_PUBLIC_IP", "RUNPOD_TCP_PORT_22",
]
SECRET_MARKERS = ("KEY", "TOKEN", "SECRET", "PASSWORD", "PASS", "CREDENTIAL")


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def run_cmd(cmd: List[str], timeout: float = 30.0) -> Dict[str, Any]:
    """Uruchamia polecenie diagnostyczne; błędy są zapisywane, a nie zgłaszane."""
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return {"cmd": " ".join(cmd), "returncode": p.returncode, "stdout": p.stdout.strip(), "stderr": p.stderr.strip()[-2000:]}
    except FileNotFoundError:
        return {"cmd": " ".join(cmd), "returncode": None, "error": "command not found"}
    except Exception as exc:  # pragma: no cover - diagnostyka
        return {"cmd": " ".join(cmd), "returncode": None, "error": str(exc)}


def git_info() -> Dict[str, Any]:
    commit = run_cmd(["git", "-C", str(PROJECT_ROOT), "rev-parse", "HEAD"]).get("stdout", "")
    branch = run_cmd(["git", "-C", str(PROJECT_ROOT), "rev-parse", "--abbrev-ref", "HEAD"]).get("stdout", "")
    diff_stat = run_cmd(["git", "-C", str(PROJECT_ROOT), "diff", "--stat", "HEAD"]).get("stdout", "")
    untracked = run_cmd(["git", "-C", str(PROJECT_ROOT), "status", "--porcelain"]).get("stdout", "")
    return {"commit": commit, "branch": branch, "dirty": bool(diff_stat.strip()), "diff_stat": diff_stat,
            "status_porcelain": untracked}


@contextlib.contextmanager
def _locked(path: Path) -> Iterator[None]:
    lock = path.with_suffix(path.suffix + ".lock")
    lock.parent.mkdir(parents=True, exist_ok=True)
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


def _atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


def manifest_path(results_dir: Path) -> Path:
    return Path(results_dir) / "manifest.json"


def read_manifest(results_dir: Path) -> Dict[str, Any]:
    p = manifest_path(results_dir)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def update_manifest(results_dir: Path, updates: Dict[str, Any]) -> Dict[str, Any]:
    """Scala `updates` z manifestem (płytko na poziomie kluczy najwyższych, słowniki scalane)."""
    p = manifest_path(results_dir)
    with _locked(p):
        m = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        for k, v in updates.items():
            if isinstance(v, dict) and isinstance(m.get(k), dict):
                m[k].update(v)
            else:
                m[k] = v
        m["updated_at"] = now_iso()
        _atomic_write_json(p, m)
        return m


def record_scenario_note(results_dir: Path, scenario: str, key: str, value: Any) -> None:
    """Zapisuje w manifeście informację opisującą scenariusz (np. tryb sieci, regułę sweet spot)."""
    p = manifest_path(results_dir)
    with _locked(p):
        m = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        notes = m.setdefault("scenario_notes", {}).setdefault(scenario, {})
        notes[key] = value
        m["updated_at"] = now_iso()
        _atomic_write_json(p, m)


def file_hashes(results_dir: Path) -> Dict[str, str]:
    out = {}
    for f in sorted(Path(results_dir).rglob("*")):
        if f.is_file() and f.name not in ("manifest.json",) and not f.name.endswith((".lock", ".tmp")):
            out[str(f.relative_to(results_dir))] = sha256_file(f)
    return out


def ollama_models(base_url: str) -> Dict[str, Any]:
    """Lista modeli z /api/tags oraz szczegóły (/api/show) z digestami."""
    import httpx

    out: Dict[str, Any] = {}
    try:
        tags = httpx.get(f"{base_url}/api/tags", timeout=10).json()
        out["tags"] = tags
        for m in tags.get("models", []):
            try:
                show = httpx.post(f"{base_url}/api/show", json={"model": m["name"]}, timeout=20).json()
                out.setdefault("show", {})[m["name"]] = {
                    "digest": m.get("digest"),
                    "details": show.get("details"),
                    "model_info_context_length": {k: v for k, v in (show.get("model_info") or {}).items() if "context_length" in k},
                    "modified_at": m.get("modified_at"),
                }
            except Exception as exc:
                out.setdefault("show", {})[m["name"]] = {"error": str(exc)}
        ver = httpx.get(f"{base_url}/api/version", timeout=10).json()
        out["version"] = ver.get("version")
    except Exception as exc:
        out["error"] = str(exc)
    return out


def capabilities() -> Dict[str, Any]:
    """Wykrywa Dockera, uprawnienie NET_ADMIN oraz narzędzie tc (bez modyfikacji systemu)."""
    caps: Dict[str, Any] = {}
    caps["docker"] = bool(shutil.which("docker")) and run_cmd(["docker", "info"], timeout=10).get("returncode") == 0
    caps["tc"] = bool(shutil.which("tc"))
    net_admin = False
    try:
        status = Path("/proc/self/status").read_text()
        cap_eff = int([l for l in status.splitlines() if l.startswith("CapEff:")][0].split()[1], 16)
        net_admin = bool(cap_eff & (1 << 12))  # CAP_NET_ADMIN = 12
    except Exception:
        pass
    caps["net_admin"] = net_admin
    caps["netem_possible"] = caps["tc"] and net_admin
    return caps


def capture_environment(results_dir: Path, cfg: Dict[str, Any], db: Dict[str, Any]) -> Dict[str, Any]:
    """Zapisuje raw_results/environment.json."""
    env: Dict[str, Any] = {"captured_at": now_iso(), "hostname": platform.node()}
    env["nvidia_smi"] = run_cmd(["nvidia-smi", "--query-gpu=name,memory.total,driver_version", "--format=csv"])
    env["lscpu"] = run_cmd(["lscpu"])
    env["free_b"] = run_cmd(["free", "-b"])
    env["uname"] = run_cmd(["uname", "-a"])
    env["nproc"] = run_cmd(["nproc"])
    env["df_results"] = run_cmd(["df", "-B1", str(results_dir)])
    env["runpod"] = {k: os.environ[k] for k in RUNPOD_SAFE_VARS if k in os.environ
                     and not any(s in k for s in SECRET_MARKERS)}
    env["python"] = {"version": platform.python_version(), "executable": os.sys.executable}
    env["pip_freeze"] = run_cmd([os.sys.executable, "-m", "pip", "freeze"], timeout=120).get("stdout", "").splitlines()
    if not env["pip_freeze"]:
        env["pip_freeze"] = run_cmd(["uv", "pip", "freeze", "--python", os.sys.executable], timeout=120).get("stdout", "").splitlines()

    env["postgres"] = _postgres_versions(db)
    env["kafka"] = _kafka_version()
    env["ollama"] = ollama_models(cfg["llm"]["base_url"])
    env["ollama"]["cli_version"] = run_cmd(["ollama", "--version"])
    env["ollama"]["orchestrator_env"] = {k: v for k, v in os.environ.items() if k.startswith("OLLAMA_")}
    env["capabilities"] = capabilities()
    p = Path(results_dir) / "environment.json"
    _atomic_write_json(p, env)
    return env


def _postgres_versions(db: Dict[str, Any]) -> Dict[str, Any]:
    import asyncio

    import asyncpg

    async def q() -> Dict[str, Any]:
        conn = await asyncpg.connect(host=db["host"], port=db["port"], user=db["user"], password=db["password"], database=db["database"])
        try:
            version = await conn.fetchval("SELECT version()")
            ext = await conn.fetch("SELECT extname, extversion FROM pg_extension ORDER BY extname")
            settings = await conn.fetch(
                "SELECT name, setting, unit FROM pg_settings WHERE name IN "
                "('shared_buffers','work_mem','statement_timeout','max_connections','jit','max_parallel_workers_per_gather')")
            return {"version": version, "extensions": {r["extname"]: r["extversion"] for r in ext},
                    "settings": {r["name"]: f"{r['setting']}{r['unit'] or ''}" for r in settings}}
        finally:
            await conn.close()

    try:
        return asyncio.run(q())
    except Exception as exc:
        return {"error": str(exc)}


def _kafka_version() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for cand in [os.getenv("KAFKA_HOME", "/opt/kafka") + "/bin/kafka-topics.sh", "kafka-topics.sh"]:
        r = run_cmd([cand, "--version"], timeout=30)
        if r.get("returncode") == 0:
            out["kafka_topics_version"] = r["stdout"]
            break
    if "kafka_topics_version" not in out and shutil.which("docker"):
        r = run_cmd(["docker", "exec", os.getenv("KAFKA_CONTAINER", "mcp_kafka"), "/opt/kafka/bin/kafka-topics.sh", "--version"], timeout=30)
        if r.get("returncode") == 0:
            out["kafka_topics_version"] = r["stdout"]
            out["source"] = "docker exec"
    try:
        import aiokafka

        out["aiokafka"] = aiokafka.__version__
    except Exception:
        pass
    return out


def archive_existing(results_dir: Path) -> Optional[Path]:
    """Przenosi całą zawartość katalogu wyników do benchmarks/raw_results_archive/<znacznik czasu>/."""
    results_dir = Path(results_dir)
    items = [p for p in results_dir.iterdir() if p.name != ".gitkeep"] if results_dir.exists() else []
    if not items:
        return None
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = results_dir.parent / "raw_results_archive" / stamp
    dest.mkdir(parents=True, exist_ok=False)
    for p in items:
        shutil.move(str(p), str(dest / p.name))
    return dest
