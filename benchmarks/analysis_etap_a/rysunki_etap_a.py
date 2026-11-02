#!/usr/bin/env python3
"""
Etap A: ponowne wygenerowanie wyłącznie rys. 2-5 (Scenariusz A) generatorem z plot_generators/,
bez regenerowania rysunków spoza zakresu korekty. Rys. 10 nie jest generowany: jego plik
źródłowy (llm_inference_latencies.csv) to próbki np.random, a nie pomiar (krok0_rozpoznanie.py, pkt 4).
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks" / "plot_generators"))

from generate_thesis_plots import ThesisPlotGenerator  # noqa: E402


def main() -> None:
    gen = ThesisPlotGenerator(
        raw_dir=ROOT / "benchmarks" / "raw_results",
        output_dir=ROOT / "doc" / "rysunki",
        mirror_dirs=[ROOT / "doc" / "figures", ROOT / "figures", ROOT / "benchmarks" / "plots"],
        save_png=True,
    )
    gen.generate_schema_recall_precision()   # rys. 2
    gen.generate_text2sql_accuracy()         # rys. 3
    gen.generate_error_decomposition_tree()  # rys. 4
    gen.generate_ablation_mexp()             # rys. 5


if __name__ == "__main__":
    main()
