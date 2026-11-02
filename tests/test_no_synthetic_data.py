"""
Część 2, pkt 4: skrypty wykresów i analizy nie mogą losować ani generować danych.
Wzorce: np.random, random., default_rng, uniform(, normal(. Dozwolone wyjątki (bootstrap z ziarnem
z konfiguracji) są wymienione jawnie w ALLOWED jako (plik, fragment linii).
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SCANNED = [
    *sorted((ROOT / "benchmarks" / "plot_generators").glob("*.py")),
    ROOT / "benchmarks" / "statistical_analysis.py",
    ROOT / "benchmarks" / "make_summary.py",
    ROOT / "benchmarks" / "validate_run.py",
    ROOT / "benchmarks" / "common" / "stats.py",
    ROOT / "benchmarks" / "common" / "rules.py",
    *sorted((ROOT / "scripts").glob("*.py")),
]
PATTERN = re.compile(r"np\.random|numpy\.random|(?<![\w.])random\.|default_rng|uniform\(|normal\(")

# Dozwolone: bootstrap z ziarnem przekazanym z konfiguracji (seeds.bootstrap)
ALLOWED = {
    ("benchmarks/common/stats.py", "rng = np.random.default_rng(seed)"),
    ("benchmarks/statistical_analysis.py", "rng = np.random.default_rng(seed)"),
}


def test_no_random_generation_in_plot_and_analysis_scripts():
    violations = []
    for f in SCANNED:
        if not f.exists():
            continue
        rel = str(f.relative_to(ROOT))
        for i, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            code = line.split("#", 1)[0]
            if PATTERN.search(code) and not any(rel == a and frag in line for a, frag in ALLOWED):
                violations.append(f"{rel}:{i}: {line.strip()}")
    assert not violations, "Losowanie w skryptach wykresów/analizy:\n" + "\n".join(violations)


def test_legacy_synthetic_generators_removed():
    assert not (ROOT / "benchmarks/plot_generators/generate_publication_plots.py").exists()
    assert not (ROOT / "scripts/generate_streaming_and_security_plots.py").exists()
    assert not (ROOT / "scripts/generate_schema_discovery_and_pagination_plots.py").exists()
