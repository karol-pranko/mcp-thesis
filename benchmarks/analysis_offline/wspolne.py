"""
Wspólne narzędzia analiz wtórnych (offline) danych rundy 20261004T120209_d347d2.

Zasady: skrypty tylko czytają pliki rundy (results-remote-new-sunday/) i zapisują wyniki w podkatalogu
derived/; żaden plik oryginalny nie jest nadpisywany. Każdy skrypt dopisuje do derived/MANIFEST.json
wpis: skrypt, jego SHA-256, wersja kodu repozytorium, wejścia i wyjścia z SHA-256, czas wykonania.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[2]
RUN = ROOT / "results-remote-new-sunday"
DERIVED = RUN / "derived"
DERIVED.mkdir(exist_ok=True)
RUN_ID = json.loads((RUN / "manifest.json").read_text(encoding="utf-8"))["run_id"]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def git_rev() -> str:
    try:
        return subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    except Exception:
        return "unknown"


def record(script: str, inputs: Iterable[Path], outputs: Iterable[Path], note: str = "") -> None:
    """Dopisuje (lub zastępuje) wpis skryptu w derived/MANIFEST.json."""
    mpath = DERIVED / "MANIFEST.json"
    m = json.loads(mpath.read_text(encoding="utf-8")) if mpath.exists() else {"run_id": RUN_ID, "entries": {}}
    spath = Path(script).resolve()
    m["entries"][spath.name] = {
        "script": str(spath.relative_to(ROOT)),
        "script_sha256": sha256(spath),
        "repo_commit": git_rev(),
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "inputs": {str(Path(p).resolve().relative_to(ROOT)): sha256(Path(p)) for p in inputs},
        "outputs": {str(Path(p).resolve().relative_to(ROOT)): sha256(Path(p)) for p in outputs},
        "note": note,
    }
    mpath.write_text(json.dumps(m, indent=2, ensure_ascii=False), encoding="utf-8")


def write_json(name: str, obj) -> Path:
    p = DERIVED / name
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return p
