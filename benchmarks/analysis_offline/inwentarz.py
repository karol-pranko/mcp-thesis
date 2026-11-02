"""Inwentarz plików rundy: kolumny, liczba rekordów, typ (wejście dla analiz O1-O9). Wyjście: derived/inwentarz.json"""
import json
from pathlib import Path

import pandas as pd

from wspolne import RUN, record, write_json

out = {}
for p in sorted(RUN.iterdir()):
    if p.is_dir() or p.name.endswith(".lock"):
        continue
    e = {"bytes": p.stat().st_size}
    try:
        if p.suffix == ".parquet":
            d = pd.read_parquet(p); e.update(rows=len(d), columns=list(d.columns))
        elif p.suffix == ".csv":
            d = pd.read_csv(p); e.update(rows=len(d), columns=list(d.columns))
        elif p.suffix == ".jsonl":
            rows = [json.loads(l) for l in p.open(encoding="utf-8")]
            e.update(rows=len(rows), columns=sorted({k for r in rows for k in r}))
        elif p.suffix == ".json":
            d = json.loads(p.read_text(encoding="utf-8"))
            e.update(keys=list(d.keys()) if isinstance(d, dict) else f"list[{len(d)}]")
    except Exception as exc:  # noqa: BLE001
        e["error"] = str(exc)
    out[p.name] = e
o = write_json("inwentarz.json", out)
record(__file__, [], [o], "inwentarz plików rundy")
for k, e in out.items():
    print(k, e.get("rows", ""), (e.get("columns") or e.get("keys") or "")[:40] if isinstance(e.get("columns") or e.get("keys"), list) else "")
