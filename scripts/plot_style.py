"""Wspólny styl wykresów do pracy: szeryfowa czcionka, bez zbędnych ramek, stonowane kolory."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure

# --- kolory ---

COLORS: Dict[str, str] = {
    # Kolor główny: Głęboki granat akademicki (MCP / Proponowane rozwiązanie)
    "primary": "#004B87",
    "mcp": "#004B87",
    
    # Alternatywy / Punkty odniesienia (Baseline)
    "baseline": "#4A5568",       # Stonowany grafit (Baseline standard)
    "baseline_alt": "#C53030",   # Kontrastowy terakotowy (Direct DB / Legacy)
    "terracotta": "#C53030",     # Synonim dla terakoty
    
    # Kolory akcentujące i warianty
    "accent_cyan": "#00A3C4",    # Morski / błękit
    "accent_orange": "#DD6B20",  # Pomarańcz / uwaga
    "accent_green": "#2F855A",   # Stonowana zieleń (sukces / oszczędność)
    "accent_purple": "#6B46C1",  # Fiolet
    
    # Tło i elementy strukturalne
    "background": "#FFFFFF",     # Czysta biel
    "grid": "#E2E8F0",           # Bardzo jasny szary do linii pomocniczych
    "border": "#CBD5E0",         # Stonowany szary do osi
    
    # Typografia
    "text": "#1A202C",           # Ciemny grafit (mniej agresywny niż #000000)
    "text_muted": "#718096",     # Wyciszony szary do adnotacji i podtytułów
}

# Wygodna sekwencja kolorów do wykresów wieloserii
PALETTE_SERIES = [
    COLORS["primary"],
    COLORS["baseline"],
    COLORS["accent_cyan"],
    COLORS["accent_orange"],
    COLORS["terracotta"],
    COLORS["accent_green"],
]

__all__ = [
    "COLORS",
    "PALETTE_SERIES",
    "set_academic_theme",
    "apply_academic_style",
    "save_figure",
    "create_academic_figure",
    "add_bar_labels",
]

# --- czcionki i rcParams ---

FONT_SERIF_CANDIDATES = [
    "DejaVu Serif",
    "Liberation Serif",
    "CMU Serif",
    "Times New Roman",
    "Times",
    "serif",
]

def set_academic_theme() -> None:
    """Konfiguruje globalne parametry matplotlib (rcParams) pod publikację akademicką."""
    mpl.rcParams.update({
        # Czcionki - krój szeryfowy spójny z LaTeXem
        "font.family": "serif",
        "font.serif": FONT_SERIF_CANDIDATES,
        "font.size": 10.0,
        "text.color": COLORS["text"],
        
        # Zgodność wektorowa z LaTeX / PDF
        "pdf.fonttype": 42,           # TrueType font embedding (brak rasteryzacji)
        "ps.fonttype": 42,
        "mathtext.fontset": "dejavuserif",
        
        # Rozmiary etykiet i elementów
        "axes.titlesize": 12.0,
        "axes.titleweight": "bold",
        "axes.titlecolor": COLORS["text"],
        "axes.titlepad": 12.0,
        
        "axes.labelsize": 11.0,
        "axes.labelweight": "normal",
        "axes.labelcolor": COLORS["text"],
        "axes.labelpad": 8.0,
        
        "xtick.labelsize": 10.0,
        "ytick.labelsize": 10.0,
        "xtick.color": COLORS["text"],
        "ytick.color": COLORS["text"],
        "xtick.direction": "out",
        "ytick.direction": "out",
        "xtick.major.size": 3.5,
        "ytick.major.size": 3.5,
        "xtick.major.width": 0.8,
        "ytick.major.width": 0.8,
        
        # Płótno i osie
        "figure.facecolor": COLORS["background"],
        "figure.edgecolor": COLORS["background"],
        "figure.dpi": 300,
        "axes.facecolor": COLORS["background"],
        "axes.edgecolor": COLORS["border"],
        "axes.linewidth": 0.8,
        "axes.axisbelow": True,       # Siatka zawsze pod słupkami/wykresem
        
        # Siatka
        "axes.grid": True,
        "grid.color": COLORS["grid"],
        "grid.linestyle": "--",
        "grid.linewidth": 0.7,
        "grid.alpha": 0.6,
        
        # Legenda
        "legend.fontsize": 10.0,
        "legend.title_fontsize": 10.0,
        "legend.frameon": True,
        "legend.framealpha": 0.95,
        "legend.facecolor": COLORS["background"],
        "legend.edgecolor": COLORS["grid"],
        "legend.fancybox": False,
        "legend.borderpad": 0.5,
        
        # Domyślny zapis
        "savefig.dpi": 300,
        "savefig.facecolor": COLORS["background"],
        "savefig.edgecolor": COLORS["background"],
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.05,
    })

# Aktywuj styl automatycznie przy imporcie
set_academic_theme()

# --- osie ---

def apply_academic_style(
    ax: Axes,
    title: Optional[str] = None,
    xlabel: Optional[str] = None,
    ylabel: Optional[str] = None,
    grid: Union[str, bool] = "y",
    despine: bool = True,
    subtitle: Optional[str] = None,
) -> Axes:
    """Aplikuje powtarzalny, minimalistyczny styl akademicki do osi wykresu.

    Args:
        ax: Obiekt Axes matplotlib.
        title: Opcjonalny tytuł wykresu (12pt semibold).
        xlabel: Opcjonalna etykieta osi X (11pt).
        ylabel: Opcjonalna etykieta osi Y (11pt).
        grid: Tryb siatki: 'y' (tylko pozioma - zalecana w tabelach/słupkach),
              'x' (pionowa), 'both' (obie), False/None (brak).
        despine: Czy usunąć górną i prawą ramkę (usuwa zbędny szum wykresu).
        subtitle: Opcjonalny subtelny podtytuł umieszczony pod tytułem.

    Returns:
        Zmodyfikowany obiekt Axes.
    """
    ax.set_facecolor(COLORS["background"])

    # tytuł i etykiety
    if title:
        ax.set_title(title, fontsize=12, fontweight="bold", color=COLORS["text"], pad=14 if subtitle else 10)
    if subtitle:
        # Podtytuł jako tekst w lewym górnym rogu ponad wykresem
        ax.text(
            0.0,
            1.02,
            subtitle,
            transform=ax.transAxes,
            fontsize=9.5,
            color=COLORS["text_muted"],
            verticalalignment="bottom",
            horizontalalignment="left",
        )
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=11, color=COLORS["text"], labelpad=8)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=11, color=COLORS["text"], labelpad=8)

    # ticks i etykiety osi
    ax.tick_params(
        axis="both",
        which="both",
        labelsize=10,
        colors=COLORS["text"],
        direction="out",
        length=3.5,
        width=0.8,
    )

    # usuwanie zbędnych ramek (despine)
    if despine:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(True)
        ax.spines["bottom"].set_visible(True)
        ax.spines["left"].set_color(COLORS["border"])
        ax.spines["bottom"].set_color(COLORS["border"])
        ax.spines["left"].set_linewidth(0.8)
        ax.spines["bottom"].set_linewidth(0.8)
    else:
        for spine in ax.spines.values():
            spine.set_color(COLORS["border"])
            spine.set_linewidth(0.8)

    # formatowanie siatki
    ax.set_axisbelow(True)
    if grid is False or grid is None:
        ax.grid(False)
    elif grid == "y":
        ax.grid(True, axis="y", color=COLORS["grid"], linestyle="--", linewidth=0.7, alpha=0.6)
        ax.grid(False, axis="x")
    elif grid == "x":
        ax.grid(True, axis="x", color=COLORS["grid"], linestyle="--", linewidth=0.7, alpha=0.6)
        ax.grid(False, axis="y")
    elif grid == "both":
        ax.grid(True, axis="both", color=COLORS["grid"], linestyle="--", linewidth=0.7, alpha=0.6)

    return ax

# --- zapis do pliku ---

def save_figure(
    fig: Figure,
    filename_base: str,
    output_dir: Union[str, Path] = "results/plots",
    dpi: int = 300,
    close: bool = True,
    transparent: bool = False,
) -> Dict[str, Path]:
    """Zapisuje wykres jednocześnie do PDF (wektorowy LaTeX) oraz PNG (300 DPI podgląd).

    Tworzy katalog docelowy (domyślnie results/plots/) jeśli nie istnieje.

    Args:
        fig: Obiekt figury matplotlib.
        filename_base: Nazwa bazowa pliku (np. 'benchmark_latency', bez rozszerzenia).
        output_dir: Katalog docelowy zapisu.
        dpi: Rozdzielczość dla pliku rastrowego PNG (domyślnie 300).
        close: Czy zamknąć figurę po zapisie (zwalnia pamięć).
        transparent: Czy tło ma być przezroczyste (domyślnie False - czysta biel).

    Returns:
        Słownik ze ścieżkami: {'pdf': Path(...), 'png': Path(...)}.
    """
    # Znajdź katalog bazowy projektu (jeśli uruchamiano np. z podkatalogu)
    out_path = Path(output_dir)
    if not out_path.is_absolute():
        # Szukaj katalogu nadrzędnego repozytorium
        cwd = Path.cwd()
        if (cwd / "results").exists():
            out_path = cwd / output_dir
        elif (cwd.parent / "results").exists():
            out_path = cwd.parent / output_dir
        else:
            out_path = cwd / output_dir

    out_path.mkdir(parents=True, exist_ok=True)

    # Oczyść nazwę bazową ze zbędnych rozszerzeń
    clean_base = filename_base.strip()
    for ext in [".png", ".pdf", ".svg", ".eps"]:
        if clean_base.lower().endswith(ext):
            clean_base = clean_base[:-len(ext)]

    pdf_file = out_path / f"{clean_base}.pdf"
    png_file = out_path / f"{clean_base}.png"

    # PDF do LaTeX-a
    fig.savefig(
        pdf_file,
        format="pdf",
        bbox_inches="tight",
        pad_inches=0.05,
        facecolor="none" if transparent else COLORS["background"],
        edgecolor="none",
    )

    # PNG do podglądu i na slajdy
    fig.savefig(
        png_file,
        format="png",
        dpi=dpi,
        bbox_inches="tight",
        pad_inches=0.05,
        facecolor="none" if transparent else COLORS["background"],
        edgecolor="none",
    )

    if close:
        plt.close(fig)

    print(f"[PlotStyle] Zapisano wykres do:")
    print(f"  - PDF (LaTeX): {pdf_file}")
    print(f"  - PNG ({dpi} DPI): {png_file}")

    return {"pdf": pdf_file, "png": png_file}

# --- pomocnicze ---

def create_academic_figure(
    width: float = 6.2,
    aspect: float = 1.618,  # Złota proporcja
    nrows: int = 1,
    ncols: int = 1,
    sharex: bool = False,
    sharey: bool = False,
) -> Tuple[Figure, Any]:
    """Tworzy figurę o proporcjach zoptymalizowanych pod szerokość kolumny pracy magisterskiej.

    Domyślna szerokość 6.2 cala idealnie wypełnia szerokość tekstu na stronie A4
    przy standardowych marginesach LaTeXa (2.5 cm).
    """
    height = (width / ncols) / aspect * nrows
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(width, height), sharex=sharex, sharey=sharey)
    return fig, axes

def add_bar_labels(
    ax: Axes,
    fmt: str = "%.1f",
    padding: float = 3.0,
    fontsize: float = 9.0,
    color: Optional[str] = None,
) -> None:
    """Elegancko dodaje etykiety wartości nad słupkami wykresu słupkowego."""
    text_color = color or COLORS["text"]
    for container in ax.containers:
        ax.bar_label(
            container,
            fmt=fmt,
            padding=padding,
            fontsize=fontsize,
            color=text_color,
            family="serif",
        )

# --- podgląd: python scripts/plot_style.py ---

if __name__ == "__main__":
    import numpy as np

    print("Generowanie demonstracyjnego wykresu w stylu akademickim...")
    
    fig, (ax1, ax2) = create_academic_figure(width=7.2, aspect=1.3, nrows=1, ncols=2)

    # wykres słupkowy: Porównanie MCP vs Baseline
    categories = ["10 Tab.", "50 Tab.", "100 Tab.", "200 Tab."]
    x = np.arange(len(categories))
    width = 0.35

    mcp_latency = [42.1, 48.5, 52.3, 56.8]
    baseline_latency = [65.4, 112.0, 198.3, 340.5]

    rects1 = ax1.bar(
        x - width / 2,
        mcp_latency,
        width,
        label="MCP (Proponowana metoda)",
        color=COLORS["primary"],
        edgecolor=COLORS["background"],
        linewidth=0.8,
    )
    rects2 = ax1.bar(
        x + width / 2,
        baseline_latency,
        width,
        label="Direct DB (Baseline)",
        color=COLORS["baseline"],
        edgecolor=COLORS["background"],
        linewidth=0.8,
    )

    apply_academic_style(
        ax1,
        title="Skalowalność Czasu Odpowiedzi",
        xlabel="Liczba tabel w schemacie bazy",
        ylabel="Średni czas odpowiedzi [ms]",
        grid="y",
    )
    ax1.set_xticks(x)
    ax1.set_xticklabels(categories)
    ax1.set_ylim(0, 420)
    ax1.legend(loc="upper left", framealpha=0.95)
    add_bar_labels(ax1, fmt="%.0f ms", fontsize=8.5, padding=4.0)

    # wykres liniowy: Oszczędność tokenów / kosztów
    tokens_k = [10, 50, 100, 200]
    tokens_baseline = [12.4, 58.2, 114.8, 235.1]
    tokens_mcp = [2.1, 3.8, 5.2, 7.9]

    ax2.plot(
        tokens_k,
        tokens_baseline,
        marker="o",
        color=COLORS["terracotta"],
        label="Direct Prompting (Baseline)",
        linewidth=2.0,
        markersize=6,
    )
    ax2.plot(
        tokens_k,
        tokens_mcp,
        marker="s",
        color=COLORS["accent_cyan"],
        label="MCP Schema Tool",
        linewidth=2.0,
        markersize=6,
    )

    apply_academic_style(
        ax2,
        title="Zużycie Kontekstu (Tokeny)",
        xlabel="Liczba tabel w bazie danych",
        ylabel="Liczba tokenów [kTokens]",
        grid="y",
    )
    ax2.set_ylim(0, 260)
    ax2.legend(loc="upper left", framealpha=0.95)

    plt.tight_layout(w_pad=2.5)

    # Zapisz demonstracyjny wykres
    result = save_figure(fig, "style_showcase_demo", output_dir="results/plots")
    print(f"Pomyślnie wygenerowano wykres demonstracyjny: {result}")
