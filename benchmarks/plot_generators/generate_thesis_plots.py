#!/usr/bin/env python3
"""
Generator wykresów pracy wyłącznie z plików katalogu wyników jednej rundy (RESULTS_DIR).

Zasady:
- skrypt nie generuje, nie losuje, nie interpoluje i nie wpisuje na sztywno żadnych wartości wyników;
- każda adnotacja (krotność, p-value, sweet spot, S*) jest wyliczana z plików tej samej rundy;
- brak pliku wejściowego kończy wykres błędem; skrypt zwraca kod != 0, jeśli którykolwiek
  z wybranych wykresów nie powstał.

Wykresy (PDF + PNG w <output-dir>, domyślnie <raw-dir>/plots):
  fig_schema_recall_precision  schema_retrieval.parquet, statistical_hypothesis_tests.csv
  fig_pareto_recall_tokens     schema_retrieval.parquet
  fig_text2sql_accuracy        schema_quality_summary.csv
  fig_error_decomposition_tree schema_error_decomposition.csv
  fig_ablation_mexp            ablation_m_exp_results.csv, ablation_m_exp_sweet_spot.json
  fig_ttft_scaling             ttft_scaling.parquet
  fig_oob_crossover_rtt        oob_crossover_summary.csv, oob_crossover_metrics.json
  fig_pagination_depth_1m      pagination_advanced_1m.csv
  fig_concurrency_rps_lag      concurrency_summary.csv, concurrency_h3_tests.json
  fig_streaming_sse_vs_polling streaming_events.csv, streaming_idle_bandwidth.csv
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Callable, Dict, List

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[2]
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", stream=sys.stdout)
logger = logging.getLogger("thesis_plots")
for _n in ("fontTools", "matplotlib", "PIL"):
    logging.getLogger(_n).setLevel(logging.WARNING)

COLORS: Dict[str, str] = {
    "primary": "#004B87", "baseline": "#4A5568", "terracotta": "#C53030", "accent_cyan": "#00A3C4",
    "accent_purple": "#6B46C1", "accent_orange": "#DD6B20", "accent_green": "#2F855A",
    "border": "#CBD5E0", "bg_box": "#F7FAFC",
}
STRATEGY_LABEL = {"DomainScoped": "Domain-Scoped (15 tabel)", "BM25": "BM25", "Dense": "Dense-only k-NN",
                  "RRF": "Hybrid RRF", "Graph": "Ekspansja grafowa"}
PROFILE_LABEL = {"small": "3B", "large": "7B"}
MODE_LABEL = {"IN_BAND_BASE64": "In-Band (Base64 w ramce JSON-RPC)", "OOB_POSIX_SHM": "OOB: pamięć współdzielona (SHM)",
              "OOB_HTTP_STREAM": "OOB: obiekt w magazynie + podpisany URI"}
NET_LABEL = {"loopback": "Jeden węzeł (zmierzony)", "lan": "LAN (modelowany: RTT 1 ms, 1000 Mb/s)",
             "wan": "WAN (modelowany: RTT 30 ms, 250 Mb/s)"}
PAG_LABEL = {"Indexed Keyset B-Tree": "Keyset z indeksem B-drzewa", "Offset/Limit": "OFFSET/LIMIT",
             "Unindexed Keyset": "Keyset bez indeksu"}
ERR_LABEL = {"TRUNCATION": "obcięcie\nodpowiedzi", "SYNTAX_ERROR": "błąd\nskładni", "TABLE_MISSING": "brak\ntabeli",
             "COLUMN_HALLUCINATION": "nieistniejąca\nkolumna", "JOIN_CONDITION_MISMATCH": "błędne\nzłączenie",
             "SEMANTIC_MISMATCH": "inny wynik\n(Valid SQL)"}


def pl(x: float, d: int = 1) -> str:
    """Liczba w formacie polskim (przecinek dziesiętny, bez separatora tysięcy w wykresach)."""
    return f"{x:.{d}f}".replace(".", ",")


def pl_axes(*axes) -> None:
    """Polski separator dziesiętny na osiach liniowych."""
    from matplotlib.ticker import FuncFormatter
    for ax in axes:
        for axis, scale in ((ax.xaxis, ax.get_xscale()), (ax.yaxis, ax.get_yscale())):
            if scale == "linear":
                axis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:g}".replace(".", ",")))
STRATEGY_COLOR = {"DomainScoped": COLORS["terracotta"], "BM25": COLORS["baseline"], "Dense": COLORS["accent_purple"],
                  "RRF": COLORS["accent_cyan"], "Graph": COLORS["primary"]}
BOX = dict(boxstyle="round,pad=0.35", facecolor=COLORS["bg_box"], edgecolor=COLORS["border"], lw=0.8)


# Scenariusze, z których pochodzą dane wykresu (używane przez --from-manifest i validate_run.py)
PLOT_SOURCES: Dict[str, List[str]] = {
    "fig_schema_recall_precision": ["schema_quality", "statistics"], "fig_pareto_recall_tokens": ["schema_quality"], "fig_text2sql_accuracy": ["schema_quality"],
    "fig_error_decomposition_tree": ["schema_quality"], "fig_ablation_mexp": ["ablation"], "fig_ttft_scaling": ["ttft_scaling"],
    "fig_oob_crossover_rtt": ["oob"], "fig_pagination_depth_1m": ["pagination"], "fig_concurrency_rps_lag": ["concurrency"],
    "fig_streaming_sse_vs_polling": ["streaming"],
}


def plots_for_completed(manifest: Dict) -> List[str]:
    done = {k for k, v in manifest.get("scenarios", {}).items() if v.get("status") == "completed"}
    return [p for p, src in PLOT_SOURCES.items() if set(src) <= done]


class MissingInput(FileNotFoundError):
    pass


def fmt_p(p: float) -> str:
    if p is None or not np.isfinite(p):
        return "n/d"
    if p >= 0.001:
        return f"{p:.3f}".replace(".", "{,}")
    m, e = f"{p:.2e}".split("e")
    return rf"{m.replace('.', '{,}')} \times 10^{{{int(e)}}}"


class Plotter:
    def __init__(self, data_dir: Path, out_dir: Path) -> None:
        self.data = Path(data_dir)
        self.out = Path(out_dir)
        self.out.mkdir(parents=True, exist_ok=True)
        plt.rcParams.update({"font.family": "serif", "font.size": 12, "axes.grid": True, "grid.alpha": 0.3,
                             "pdf.fonttype": 42, "savefig.bbox": "tight"})

    def need(self, name: str) -> Path:
        p = self.data / name
        if not p.exists():
            raise MissingInput(f"brak pliku wejściowego: {p}")
        return p

    def save(self, fig: plt.Figure, stem: str) -> Path:
        pdf = self.out / f"{stem}.pdf"
        fig.savefig(pdf)
        fig.savefig(self.out / f"{stem}.png", dpi=200)
        plt.close(fig)
        logger.info("Wygenerowano %s", pdf)
        return pdf

    # ------------------------------------------------------------------ Scenariusz A
    def schema_recall_precision(self) -> Path:
        r = pd.read_parquet(self.need("schema_retrieval.parquet"))
        tests = pd.read_csv(self.need("statistical_hypothesis_tests.csv"))
        ks = r[r.budget_variant == "k_sweep"]
        fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 5))
        for s in ["BM25", "Dense", "RRF", "Graph"]:
            d = ks[ks.strategy == s].groupby("k")[["table_recall", "table_precision"]].mean()
            if d.empty:
                continue
            a1.plot(d.index, d.table_recall * 100, marker="o", color=STRATEGY_COLOR[s], label=STRATEGY_LABEL[s])
            a2.plot(d.index, d.table_precision * 100, marker="o", color=STRATEGY_COLOR[s], label=STRATEGY_LABEL[s])
        dom = r[r.strategy == "DomainScoped"]
        if len(dom):
            a1.axhline(dom.table_recall.mean() * 100, color=STRATEGY_COLOR["DomainScoped"], ls="--", label=STRATEGY_LABEL["DomainScoped"])
            a2.axhline(dom.table_precision.mean() * 100, color=STRATEGY_COLOR["DomainScoped"], ls="--")
        eb = r[r.budget_variant == "equal_budget"]
        for s, d in eb.groupby("strategy"):
            for k, dd in d.groupby("k"):
                a1.scatter([k], [dd.table_recall.mean() * 100], marker="D", s=70, facecolor="white",
                           edgecolor=STRATEGY_COLOR[s], zorder=5, label=f"{STRATEGY_LABEL[s]}, k = {k} (równy budżet)")
        lines = [f"N = {r.query_id.nunique()} zapytań"]
        if "status" in tests:
            h1 = tests[tests.test_id.astype(str).str.startswith("H1: Graph@5 vs") & (tests.status == "reported")]
            for _, t in h1.iterrows():
                if t.get("kind") in ("rowny_budzet", "budzet_nie_mniejszy"):
                    name = t.test_id[4:].replace(" (equal_budget)", "").replace("Graph@5", "Ekspansja@5").replace(" vs ", " wobec ")
                    lines.append(rf"{name}: {pl(100 * t.mean_a)}% wobec {pl(100 * t.mean_b)}%, $p = {fmt_p(t.p_value)}$")
        a1.set_ylim(top=a1.get_ylim()[1] * 1.3)
        a1.text(0.02, 0.98, "\n".join(lines), transform=a1.transAxes, va="top", fontsize=9, bbox=BOX)
        a1.set(xlabel="k (liczba tabel z wyszukiwania)", ylabel="Table-Recall [%]", title="A. Table-Recall@k")
        a2.set(xlabel="k", ylabel="Table-Precision [%]", title="B. Table-Precision@k")
        a1.legend(fontsize=8.5, loc="lower right")
        for a in (a1, a2):
            a.set_xticks([3, 5, 8, 10, 20])
        pl_axes(a1, a2)
        return self.save(fig, "fig_schema_recall_precision")

    def pareto_recall_tokens(self) -> Path:
        r = pd.read_parquet(self.need("schema_retrieval.parquet"))
        pts = r.groupby(["strategy", "k"]).agg(recall=("table_recall", "mean"), tokens=("ddl_tokens", "mean")).reset_index()
        fig, ax = plt.subplots(figsize=(10, 5.6))
        for s in ["BM25", "Dense", "RRF", "Graph", "DomainScoped"]:
            d = pts[pts.strategy == s].sort_values("tokens")
            if d.empty:
                continue
            ax.plot(d.tokens, d.recall * 100, marker="D" if s == "DomainScoped" else "o", ls="-" if len(d) > 1 else "",
                    color=STRATEGY_COLOR[s], label=STRATEGY_LABEL[s])
            for _, row in d.iterrows():
                ax.annotate(f"k = {row.k}", (row.tokens, row.recall * 100), xytext=(4, -12), textcoords="offset points",
                            fontsize=8.5, color=STRATEGY_COLOR[s])
        ax.set(xlabel="Średnia liczba tokenów bloku DDL (cl100k_base)", ylabel="Table-Recall [%]",
               title="Czułość selekcji a objętość kontekstu")
        ax.legend(fontsize=10, loc="lower right")
        pl_axes(ax)
        return self.save(fig, "fig_pareto_recall_tokens")

    def text2sql_accuracy(self) -> Path:
        s = pd.read_csv(self.need("schema_quality_summary.csv"))
        short = {"DomainScoped": "Domain-Scoped", "Dense": "Dense-only", "Graph": "Ekspansja\ngrafowa"}
        s["label"] = s.apply(lambda x: f"{short.get(x.strategy, STRATEGY_LABEL.get(x.strategy, x.strategy))}\n(k = {x.k}"
                             f"{',\nrówny budżet' if x.budget_variant == 'equal_budget' else ''})", axis=1)
        profiles = [p for p in ("small", "large") if p in set(s.profile)]
        labels = list(dict.fromkeys(s.label))
        x = np.arange(len(labels))
        w = 0.8 / max(1, len(profiles))
        fig, axes = plt.subplots(2, 1, figsize=(13, 9.5), sharex=True)
        for ax, col, title in ((axes[0], "valid_sql_pct", "A. Valid SQL [%] (EXPLAIN i wykonanie bez błędu)"),
                               (axes[1], "execution_accuracy_pct", "B. Execution Accuracy (EX) [%]")):
            for i, p in enumerate(profiles):
                d = s[s.profile == p].set_index("label").reindex(labels)
                bars = ax.bar(x + (i - (len(profiles) - 1) / 2) * w, d[col], w,
                              label=f"profil {PROFILE_LABEL.get(p, p)} ({d.model.dropna().iloc[0]})")
                for b, v in zip(bars, d[col]):
                    if np.isfinite(v):
                        ax.annotate(pl(v), (b.get_x() + b.get_width() / 2, v), xytext=(0, 2), textcoords="offset points",
                                    ha="center", va="bottom", fontsize=9)
            ax.set_ylim(0, max(s[col]) * 1.18)
            ax.set_title(title)
            pl_axes(ax)
        axes[1].set_xticks(x, labels, fontsize=9.5)
        axes[0].legend(fontsize=9.5, loc="upper left")
        return self.save(fig, "fig_text2sql_accuracy")

    def error_decomposition(self) -> Path:
        d = pd.read_csv(self.need("schema_error_decomposition.csv"))
        # kategorie taksonomii z podrozdziału 6.1.1, które wystąpiły w danych (GT_EMPTY nie dotyczy: brak pustych GT)
        cats = [c for c in ERR_LABEL if c in set(d.error_category)]
        profiles = [p for p in ("small", "large") if p in set(d.profile)]
        if not profiles:
            raise MissingInput("brak rekordów z EX = False w schema_error_decomposition.csv")
        order = [("DomainScoped", 15), ("BM25", 5), ("Dense", 5), ("RRF", 5), ("Graph", 5), ("BM25", 8), ("RRF", 8)]
        fig, axes = plt.subplots(len(profiles), 1, figsize=(15, 5.0 * len(profiles)), squeeze=False, sharex=True)
        for ax, p in zip(axes[:, 0], profiles):
            sub = d[d.profile == p]
            variants = [(s_, k) for s_, k in order if ((sub.strategy == s_) & (sub.k == k)).any()]
            x = np.arange(len(cats))
            w = 0.84 / max(1, len(variants))
            for i, (s_, k) in enumerate(variants):
                ss = sub[(sub.strategy == s_) & (sub.k == k)].set_index("error_category").reindex(cats)
                tot = int(ss["ex_false_total"].dropna().iloc[0]) if ss["ex_false_total"].notna().any() else 0
                ax.bar(x + (i - (len(variants) - 1) / 2) * w, ss["percentage"].fillna(0), w, color=STRATEGY_COLOR[s_],
                       hatch="//" if k == 8 else None, edgecolor="white" if k != 8 else "black", linewidth=0.3,
                       label=f"{STRATEGY_LABEL[s_]}, k = {k}{' (równy budżet)' if k == 8 else ''}: {tot} niepoprawnych")
            ax.set_title(f"Profil {PROFILE_LABEL.get(p, p)}")
            ax.set_ylabel("% niepoprawnych zapytań strategii")
            ax.legend(fontsize=9, loc="upper left", bbox_to_anchor=(1.01, 1.0), title="liczba zapytań z EX = fałsz")
            pl_axes(ax)
        axes[-1][0].set_xticks(np.arange(len(cats)), [ERR_LABEL[c] for c in cats], fontsize=10)
        return self.save(fig, "fig_error_decomposition_tree")

    def ablation(self) -> Path:
        a = pd.read_csv(self.need("ablation_m_exp_results.csv")).sort_values("m_exp")
        ss = json.loads(self.need("ablation_m_exp_sweet_spot.json").read_text(encoding="utf-8"))
        fig, ax1 = plt.subplots(figsize=(9.5, 5.2))
        ax1.plot(a.m_exp, a.table_recall_at_5 * 100, marker="s", color=COLORS["primary"], label="Table-Recall@5 [%] (oś lewa)")
        ax2 = ax1.twinx()
        ax2.plot(a.m_exp, a.avg_ddl_tokens_uniform_format, marker="D", ls="--", color=COLORS["accent_orange"], label="średnia liczba tokenów DDL (oś prawa)")
        ax1.axvline(ss["m_exp"], color=COLORS["accent_green"], ls="-.")
        gain = ss.get("gain_to_next_pp")
        h1, l1 = ax1.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax1.legend(h1 + h2, l1 + l2, loc="center right", fontsize=8)
        ax1.text(0.97, 0.12, "Punkt nasycenia (reguła: przyrost czułości do następnej\n"
                 f"badanej wartości < {pl(ss['threshold_pp'])} punktu proc.): " + rf"$M_{{\mathrm{{exp}}}} = {ss['m_exp']}$" + "\n"
                 f"Table-Recall@5 = {pl(ss['recall_pct'])}%" + (f", przyrost = {pl(gain, 2)} punktu proc." if gain is not None else ""),
                 transform=ax1.transAxes, fontsize=8, bbox=BOX, ha="right")
        ax1.set(xlabel=r"$M_{\mathrm{exp}}$ (maksymalna liczba tabel mostkujących)", ylabel="Table-Recall@5 [%]")
        ax2.set_ylabel("Średnia liczba tokenów DDL")
        ax2.grid(False)
        ax1.set_xticks(a.m_exp)
        pl_axes(ax1, ax2)
        return self.save(fig, "fig_ablation_mexp")

    def ttft_scaling(self) -> Path:
        d = pd.read_parquet(self.need("ttft_scaling.parquet"))
        d = d[~d.warmup]
        fig, ax = plt.subplots(figsize=(9.5, 5.2))
        for p, sub in d.groupby("profile"):
            # punkt pomiarowy = docelowy rozmiar DDL; oś X = mediana prompt_eval_count w punkcie
            # (unikalny identyfikator w prompcie zmienia liczbę tokenów o pojedyncze sztuki między powtórzeniami)
            g = sub.groupby("target_ddl_tokens")
            xs = g["prompt_eval_count"].median()
            med = g["ttft_ms"].median()
            ax.errorbar(xs.values, med.values, yerr=[med - g["ttft_ms"].quantile(0.25), g["ttft_ms"].quantile(0.75) - med],
                        marker="o", capsize=3, label=f"profil {PROFILE_LABEL.get(p, p)} ({sub.model.iloc[0]}), n = " + (f"{g.size().min()}" if g.size().min() == g.size().max()
                                                                                   else f"{g.size().min()}–{g.size().max()}") + " na punkt")
        ax.set(xlabel="Liczba tokenów promptu (prompt_eval_count)", ylabel="TTFT [ms] (mediana i rozstęp międzykwartylowy)",
               title="Czas do pierwszego tokenu w funkcji rozmiaru promptu")
        ax.legend(fontsize=10)
        pl_axes(ax)
        return self.save(fig, "fig_ttft_scaling")

    # ------------------------------------------------------------------ B1, B2, C1, C2
    def oob(self) -> Path:
        s = pd.read_csv(self.need("oob_crossover_summary.csv"))
        m = json.loads(self.need("oob_crossover_metrics.json").read_text(encoding="utf-8"))
        profiles = [p for p in ("loopback", "lan", "wan") if p in set(s.network_profile)]
        fig, axes = plt.subplots(1, len(profiles), figsize=(6.2 * len(profiles), 5.4), squeeze=False)
        for ax, p in zip(axes[0], profiles):
            sub = s[s.network_profile == p]
            modelled = sub.measurement_mode.iloc[0] != "measured_loopback"
            for mode in ("IN_BAND_BASE64", "OOB_POSIX_SHM", "OOB_HTTP_STREAM"):
                d = sub[sub.transport_mode == mode]
                if len(d):
                    ax.plot(d.payload_size_bytes / 1024, d.median_ms, marker="o", label=MODE_LABEL[mode],
                            color={"IN_BAND_BASE64": "#1f77b4", "OOB_POSIX_SHM": "#ff7f0e", "OOB_HTTP_STREAM": "#2ca02c"}[mode])
            notes = []
            for mode, c in m["profiles"].get(p, {}).items():
                if isinstance(c, dict) and c.get("reported"):
                    short = "SHM" if mode == "OOB_POSIX_SHM" else "obiekt + URI"
                    txt = f"S* ({short}) = {pl(c['s_star_kb'])} KiB"
                    if c.get("ci95_bytes") and not modelled:
                        lo, hi = c["ci95_bytes"]
                        txt += f" [95% CI {pl(lo / 1024)}; {pl(hi / 1024)}]"
                    notes.append(txt)
            if modelled:
                notes.append("wynik modelu: zmierzone koszty obliczeń\n+ założone RTT i przepustowość")
            ax.text(0.02, 0.98, "\n".join(notes), transform=ax.transAxes, va="top", fontsize=9, bbox=BOX)
            ax.set(xscale="log", yscale="log", xlabel="Rozmiar ładunku [KiB]", ylabel="Mediana czasu przekazania [ms]",
                   title=NET_LABEL.get(p, p))
        h, l = max((ax.get_legend_handles_labels() for ax in axes[0]), key=lambda t: len(t[0]))
        fig.legend(h, l, loc="lower center", ncol=3, fontsize=10, bbox_to_anchor=(0.5, -0.06))
        return self.save(fig, "fig_oob_crossover_rtt")

    def pagination(self) -> Path:
        d = pd.read_csv(self.need("pagination_advanced_1m.csv"))
        fig, ax = plt.subplots(figsize=(10, 5.4))
        for v in ("Indexed Keyset B-Tree", "Offset/Limit", "Unindexed Keyset"):
            sub = d[d.variant == v]
            if sub.empty:
                continue
            g = sub.groupby("offset_rows")["latency_ms"]
            # oś logarytmiczna: offset 0 (pierwsza strona) rysowany w pozycji 1
            xs = np.maximum(g.median().index.values, 1)
            ax.plot(xs, g.median().values, marker="o", label=f"{PAG_LABEL[v]} (n = {g.size().min()} na punkt)")
            ax.fill_between(xs, g.min().values, g.max().values, alpha=0.15)
        med = d.groupby(["variant", "offset_rows"])["latency_ms"].median().unstack(0)
        if {"Offset/Limit", "Indexed Keyset B-Tree"} <= set(med.columns):
            deepest = med.dropna(subset=["Offset/Limit", "Indexed Keyset B-Tree"]).index.max()
            ratio = med.loc[deepest, "Offset/Limit"] / med.loc[deepest, "Indexed Keyset B-Tree"]
            ax.text(0.02, 0.98, f"offset {deepest:,}: OFFSET/LIMIT jest {pl(ratio)} razy wolniejszy od keyset".replace(",", "\u2009", 1),
                    transform=ax.transAxes, va="top", fontsize=10, bbox=BOX)
        ax.set(xscale="log", yscale="log", xlabel="Offset (liczba pomijanych wierszy; offset 0 w pozycji 1)",
               ylabel="Opóźnienie strony [ms] (mediana, min–max)")
        ax.legend(fontsize=10, loc="center left")
        return self.save(fig, "fig_pagination_depth_1m")

    def concurrency(self) -> Path:
        s = pd.read_csv(self.need("concurrency_summary.csv"))
        tests = json.loads(self.need("concurrency_h3_tests.json").read_text(encoding="utf-8"))
        nonsync = s[s.variant != "SYNC"]
        default_w = nonsync.workers.mode().iloc[0] if len(nonsync) else None
        main = s[(s.variant == "SYNC") | (s.workers == default_w)]
        fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 5))
        vcol = {"SYNC": COLORS["terracotta"], "THREAD": COLORS["accent_orange"], "POOL": COLORS["primary"]}
        for v, d in main.groupby("variant"):
            d = d.sort_values("concurrency_level")
            c = vcol.get(v)
            a1.plot(d.concurrency_level, d.requests_per_second, marker="o", color=c, label=v)
            a2.plot(d.concurrency_level, d.lag_median_ms, marker="o", color=c, label=f"{v} mediana")
            a2.plot(d.concurrency_level, d.lag_p99_ms, marker="x", ls=":", color=c, label=f"{v} p99")
        notes = [rf"M = {t['concurrency_level']}: POOL wobec SYNC, test Manna–Whitneya $p = {fmt_p(t['p_value'])}$, n = {t['n_a']}/{t['n_b']}"
                 for t in tests if t.get("reported")]
        if notes:
            a2.text(0.98, 0.02, "\n".join(notes), transform=a2.transAxes, va="bottom", ha="right", fontsize=9, bbox=BOX)
        a1.set(xscale="log", xlabel="M (zadania asyncio – klienci – w jednym procesie)", ylabel="RPS", title="A. Przepustowość")
        a2.set(xscale="log", yscale="log", xlabel="M (liczba klientów)", ylabel="Opóźnienie pętli zdarzeń [ms]",
               title="B. Opóźnienie pętli zdarzeń")
        a1.legend(fontsize=10)
        a2.legend(fontsize=8.5, loc="upper left", ncol=2)
        pl_axes(a1)
        a2.set_ylim(bottom=0.05)
        return self.save(fig, "fig_concurrency_rps_lag")

    def streaming(self) -> Path:
        e = pd.read_csv(self.need("streaming_events.csv"))
        idle = pd.read_csv(self.need("streaming_idle_bandwidth.csv")).reset_index(drop=True)
        fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 5))
        variants = list(e.variant.unique())
        data = [e[(e.variant == v) & e.detected].latency_ms.values for v in variants]
        a1.boxplot(data, tick_labels=[f"{v}\n(n={len(x)})" for v, x in zip(variants, data)])
        a1.set(yscale="log", ylabel="Opóźnienie detekcji [ms]", title="A. Publikacja → odbiór przez klienta")
        meds = {v: float(np.median(x)) for v, x in zip(variants, data) if len(x)}
        if "SSE" in meds:
            a1.text(0.02, 0.98, "\n".join(f"{v} / SSE (mediany): {meds[v] / meds['SSE']:.0f}×" for v in meds if v != "SSE"),
                    transform=a1.transAxes, va="top", fontsize=8, bbox=BOX)
        a2.bar(idle.variant, idle.bytes_per_s, color=COLORS["accent_cyan"])
        for i, row in idle.iterrows():
            a2.annotate(f"{row.bytes_per_s:.1f} B/s\n{row.requests_per_s:.2f} żądań/s", (i, row.bytes_per_s), ha="center", va="bottom", fontsize=8)
        a2.set(ylabel="Bajty / s (faktyczne, klient HTTP)", title=f"B. Ruch jałowy (okno {idle.idle_window_s.median():.0f} s)")
        return self.save(fig, "fig_streaming_sse_vs_polling")

    def all(self) -> Dict[str, Callable[[], Path]]:
        return {"fig_schema_recall_precision": self.schema_recall_precision, "fig_pareto_recall_tokens": self.pareto_recall_tokens, "fig_text2sql_accuracy": self.text2sql_accuracy,
                "fig_error_decomposition_tree": self.error_decomposition, "fig_ablation_mexp": self.ablation,
                "fig_ttft_scaling": self.ttft_scaling, "fig_oob_crossover_rtt": self.oob,
                "fig_pagination_depth_1m": self.pagination, "fig_concurrency_rps_lag": self.concurrency,
                "fig_streaming_sse_vs_polling": self.streaming}




def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw-dir", default=os.getenv("RESULTS_DIR", str(PROJECT_ROOT / "benchmarks" / "raw_results")))
    ap.add_argument("--output-dir", default=None, help="Domyślnie <raw-dir>/plots")
    ap.add_argument("--only", nargs="*", default=None)
    ap.add_argument("--from-manifest", action="store_true", help="Tylko wykresy, których scenariusze źródłowe są ukończone w manifest.json.")
    args = ap.parse_args()
    if args.from_manifest:
        args.only = plots_for_completed(json.loads((Path(args.raw_dir) / "manifest.json").read_text(encoding="utf-8")))
        logger.info("Wykresy z ukończonych scenariuszy: %s", args.only)
    out = Path(args.output_dir) if args.output_dir else Path(args.raw_dir) / "plots"
    jobs = Plotter(Path(args.raw_dir), out).all()
    failed: List[str] = []
    for n in (args.only or list(jobs)):
        try:
            jobs[n]()
        except Exception as exc:
            logger.error("Wykres %s nie powstał: %s", n, exc)
            failed.append(n)
    if failed:
        logger.error("Nie powstały wykresy: %s", failed)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
