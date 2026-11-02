#!/usr/bin/env python3
"""
Kontrola spójności makr liczbowych pracy:
  1. każde makro zdefiniowane w doc/chapters/wyniki_liczby.tex jest użyte w tekście pracy,
  2. każde makro \\wyn... użyte w tekście jest zdefiniowane,
  3. każda tabela z doc/chapters/tabele/ jest wstawiona przez \\tabelawynikow.
Kod wyjścia 1 przy jakimkolwiek naruszeniu.
"""
import os
import re
import sys
from pathlib import Path

DOC = Path(__file__).resolve().parents[2] / "doc"
defs = [d for f in ("wyniki_liczby.tex", "wyniki_wtorne.tex")
        for d in re.findall(r"\\newcommand\{\\(wyn[A-Za-z]+)\}", (DOC / "chapters" / f).read_text(encoding="utf-8"))]
sources = [p for p in (DOC / "chapters").glob("*.tex") if p.name not in ("wyniki_liczby.tex", "wyniki_wtorne.tex")]
sources.append(Path(os.path.realpath(DOC / "main.tex")))
text = "\n".join(p.read_text(encoding="utf-8") for p in sources)
used = set(re.findall(r"\\(wyn[A-Za-z]+)", text))
unused = [d for d in defs if d not in used]
undefined = sorted(used - set(defs))
tables = sorted(p.stem for p in (DOC / "chapters" / "tabele").glob("*.tex"))
missing_tables = [t for t in tables if f"\\tabelawynikow{{{t}}}" not in text]
print(f"Makra zdefiniowane: {len(defs)}, użyte: {len(set(defs) & used)}")
for name, items in (("Nieużyte makra", unused), ("Niezdefiniowane makra", undefined), ("Niewstawione tabele", missing_tables)):
    print(f"{name}: {len(items)}")
    for i in items:
        print("  ", i)
sys.exit(1 if unused or undefined or missing_tables else 0)
