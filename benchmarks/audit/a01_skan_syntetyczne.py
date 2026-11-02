#!/usr/bin/env python3
"""
Audyt, Krok 2.1: przeszukanie repozytorium pod kątem generowania danych losowych lub z wzoru
oraz wartości wpisanych na sztywno w skryptach wykresów.

Wynik: out/a01_trafienia.csv (plik, linia, wzorzec, treść).
"""
import csv
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / "out"

SCAN_DIRS = ["benchmarks", "scripts", "src", "testbed", "tests", "doc/chapters"]
SKIP = {".venv", "__pycache__", "audit", "analysis_etap_a", "raw_results", "raw_results_bak", "plots", "logs"}
EXT = {".py", ".ipynb", ".sh", ".tex"}

PATTERNS = {
    "np.random": r"np\.random\.|numpy\.random",
    "default_rng": r"default_rng",
    "random.": r"(?<![\w.])random\.(random|uniform|gauss|normalvariate|randint|choice|shuffle|seed|Random|sample|expovariate|lognormvariate)",
    "seed=": r"seed\s*[=(]",
    "normal(": r"normal\(",
    "uniform(": r"uniform\(",
    "linspace": r"linspace\(",
    "hardcoded_annotation": r"(ax\d?|plt)\.(text|annotate)\(.*\d+[.,]\d+",
    "hardcoded_pvalue": r"p\s*[=<]\s*\d+[{,.]",
    "fallback_defaults": r"(Domyślne wartości referencyjne|fallback|Fallback|dane referencyjne|referencyjne z pełnego)",
}


def main() -> None:
    rows = []
    for d in SCAN_DIRS:
        for p in (ROOT / d).rglob("*"):
            if p.suffix not in EXT or any(part in SKIP for part in p.parts):
                continue
            try:
                lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
            except Exception:
                continue
            for i, line in enumerate(lines, 1):
                for name, pat in PATTERNS.items():
                    if re.search(pat, line):
                        rows.append({"plik": str(p.relative_to(ROOT)), "linia": i, "wzorzec": name, "tresc": line.strip()[:200]})
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / "a01_trafienia.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["plik", "linia", "wzorzec", "tresc"])
        w.writeheader()
        w.writerows(rows)
    from collections import Counter

    c = Counter((r["plik"], r["wzorzec"]) for r in rows)
    for (plik, wz), n in sorted(c.items()):
        print(f"{n:4d}  {wz:22s} {plik}")


if __name__ == "__main__":
    main()
