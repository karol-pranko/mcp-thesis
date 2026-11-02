#!/usr/bin/env python3
"""
Audyt, Krok 2.2: pochodzenie każdego pliku w benchmarks/raw_results/.

Dla każdego pliku: znacznik czasu, rozmiar, liczba wierszy, kolumny z metadanymi czasu/przebiegu,
skrypty .py, które odwołują się do nazwy pliku (z rozróżnieniem zapis/odczyt), oraz porównanie
z kopiami w BACKUP raw_results, benchmarks/raw_results_bak i archiwum backup2/benchmarks_results_full.tar.gz
(jedyny przebieg z zachowanymi logami: 2026-10-02 22:54 -> 2026-10-03 04:30).

Wynik: out/a02_pliki.csv oraz wydruk.
"""
import csv
import hashlib
import re
import tarfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "benchmarks" / "raw_results"
OUT = Path(__file__).resolve().parent / "out"
CODE_DIRS = ["benchmarks", "scripts", "src", "testbed"]
SKIP = {".venv", "__pycache__", "audit", "analysis_etap_a"}
WRITE_RE = re.compile(r"to_csv|to_parquet|json\.dump|write_text|open\([^)]*['\"]w")


def md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def code_refs(stem: str):
    writers, readers = set(), set()
    for d in CODE_DIRS:
        for p in (ROOT / d).rglob("*.py"):
            if any(s in p.parts for s in SKIP):
                continue
            lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
            for i, line in enumerate(lines):
                if stem in line:
                    ctx = "\n".join(lines[max(0, i - 3): i + 6])
                    rel = f"{p.relative_to(ROOT)}:{i + 1}"
                    (writers if WRITE_RE.search(ctx) else readers).add(rel)
    return sorted(writers), sorted(readers)


def main() -> None:
    tar_path = ROOT / "backup2" / "benchmarks_results_full.tar.gz"
    tar_md5 = {}
    if tar_path.exists():
        with tarfile.open(tar_path) as t:
            for m in t.getmembers():
                if m.isfile() and "raw_results/" in m.name:
                    tar_md5[Path(m.name).name] = md5(t.extractfile(m).read())
    rows = []
    for f in sorted(RAW.iterdir()):
        if f.name.startswith(".") or f.suffix not in {".csv", ".json"}:
            continue
        data = f.read_bytes()
        text = data.decode("utf-8", errors="replace")
        header = text.splitlines()[0] if f.suffix == ".csv" and text else ""
        meta_cols = [c for c in header.split(",") if re.search(r"time|date|host|run|_id$|iso", c, re.I)]
        has_ts_values = bool(re.search(r"20\d\d-\d\d-\d\dT\d\d:\d\d", text))
        stem = f.stem
        writers, readers = code_refs(stem)
        h = md5(data)

        def cmp(other: Path) -> str:
            return "brak" if not other.exists() else ("identyczny" if md5(other.read_bytes()) == h else "rozny")

        rows.append({
            "plik": f.name,
            "mtime": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            "bajty": len(data),
            "wiersze": text.count("\n"),
            "kolumny_metadanych": ";".join(meta_cols),
            "znaczniki_czasu_w_tresci": has_ts_values,
            "zapisuje (kod)": " | ".join(writers[:4]),
            "czyta (kod)": " | ".join(readers[:4]),
            "BACKUP_raw_results": cmp(ROOT / "BACKUP raw_results" / f.name),
            "raw_results_bak": cmp(ROOT / "benchmarks" / "raw_results_bak" / f.name),
            "tar_przebieg_z_logami": ("brak" if f.name not in tar_md5 else ("identyczny" if tar_md5[f.name] == h else "rozny")),
        })
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / "a02_pliki.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    for r in rows:
        print(f"{r['plik']:45s} {r['mtime']}  ts={str(r['znaczniki_czasu_w_tresci'])[0]}  tar={r['tar_przebieg_z_logami']:10s} W: {r['zapisuje (kod)'][:110]}")


if __name__ == "__main__":
    main()
