#!/usr/bin/env python3
"""
Rysuje diagram architektury testbedu (PostgreSQL + pgvector, Kafka KRaft, serwer FastMCP,
/dev/shm, harness) i zapisuje PDF do:
- doc/figures/architektura_srodowiska.pdf
- doc/EE_dyplom__Copy_/rysunki/architektura_srodowiska.pdf
- figures/architektura_srodowiska.pdf
"""

import os
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.patches as patches

# Ustawienia czcionek i stylu wektorowego PDF
plt.rcParams.update({
    "font.family": "serif",
    "font.size": 10,
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
})

fig, ax = plt.subplots(figsize=(11, 6.8), dpi=300)
ax.set_xlim(0, 11)
ax.set_ylim(0, 6.8)
ax.axis("off")

# Kolory zgodne z motywem akademickim
C_DOCKER_BG = "#F8FAFC"
C_DOCKER_BORDER = "#94A3B8"
C_CONTAINER_BG = "#FFFFFF"
C_CONTAINER_BORDER = "#1E293B"
C_HEADER_BG = "#004B87"
C_ACCENT_PG = "#336791"
C_ACCENT_KAFKA = "#231F20"
C_ACCENT_MCP = "#0D9488"
C_ACCENT_SHM = "#D97706"
C_ACCENT_CLIENT = "#4338CA"

# główny kontener / Ramka nadrzędna: Docker Compose Testbed
rect_testbed = patches.FancyBboxPatch(
    (0.4, 0.4), 10.2, 5.9,
    boxstyle="round,pad=0.2,rounding_size=0.3",
    facecolor=C_DOCKER_BG,
    edgecolor=C_DOCKER_BORDER,
    linewidth=1.8,
    linestyle="--",
)
ax.add_patch(rect_testbed)
ax.text(0.7, 6.05, "Środowisko Uruchomieniowe Docker Compose (Linux Kernel 6.8 / AMD Ryzen 7)", 
        fontsize=11, fontweight="bold", color="#334155")

# kontener 1: PostgreSQL 16 z pgvector (mcp_postgres)
rect_pg = patches.FancyBboxPatch(
    (0.8, 1.0), 3.2, 4.6,
    boxstyle="round,pad=0.15,rounding_size=0.2",
    facecolor=C_CONTAINER_BG,
    edgecolor=C_ACCENT_PG,
    linewidth=2,
)
ax.add_patch(rect_pg)

# Nagłówek kontenera PG
rect_pg_hdr = patches.FancyBboxPatch(
    (0.8, 5.0), 3.2, 0.6,
    boxstyle="round,pad=0.1,rounding_size=0.1",
    facecolor=C_ACCENT_PG,
    edgecolor=C_ACCENT_PG,
)
ax.add_patch(rect_pg_hdr)
ax.text(2.4, 5.25, "Kontener: mcp_postgres\n(PostgreSQL 16 + pgvector)", 
        fontsize=9.5, fontweight="bold", color="#FFFFFF", ha="center", va="center")

# Wnętrze PG
pg_text = (
    "Parametry jądra:\n"
    "• shared_buffers = 1 GB (RAM)\n"
    "• work_mem = 32 MB\n"
    "• max_connections = 200\n\n"
    "Struktury danych:\n"
    "• 1000 tabel relacyjnych (ERP/CRM)\n"
    "• schema_embeddings (HNSW, d=384)\n"
    "• schema_foreign_keys (1174 FK)\n"
    "• analytics_orders (1 mln wierszy)\n\n"
    "Wolumeny & Porty:\n"
    "• pgdata -> /var/lib/postgresql/data\n"
    "• Port TCP: 5432:5432"
)
ax.text(1.0, 4.75, pg_text, fontsize=8.5, color="#1E293B", va="top", linespacing=1.25)

# kontener 2: Apache Kafka w trybie KRaft (mcp_kafka)
rect_kf = patches.FancyBboxPatch(
    (4.4, 3.7), 2.8, 2.0,
    boxstyle="round,pad=0.15,rounding_size=0.2",
    facecolor=C_CONTAINER_BG,
    edgecolor="#E11D48",
    linewidth=1.8,
)
ax.add_patch(rect_kf)

rect_kf_hdr = patches.FancyBboxPatch(
    (4.4, 5.1), 2.8, 0.55,
    boxstyle="round,pad=0.08,rounding_size=0.1",
    facecolor="#E11D48",
    edgecolor="#E11D48",
)
ax.add_patch(rect_kf_hdr)
ax.text(5.8, 5.35, "Kontener: mcp_kafka\n(Tryb KRaft / Bez ZooKeepera)", 
        fontsize=9, fontweight="bold", color="#FFFFFF", ha="center", va="center")

kf_text = (
    "• Broker & Kontroler (Node ID: 1)\n"
    "• Listeners: PLAINTEXT://:9092,\n"
    "             CONTROLLER://:9093\n"
    "• Temat: telemetry_test (3 partycje)\n"
    "• Quorum: 1@localhost:9093"
)
ax.text(4.55, 4.95, kf_text, fontsize=8, color="#1E293B", va="top", linespacing=1.2)

# moduł pamięci współdzielonej: /dev/shm (POSIX Shared Memory)
rect_shm = patches.FancyBboxPatch(
    (4.4, 1.0), 2.8, 2.2,
    boxstyle="round,pad=0.15,rounding_size=0.2",
    facecolor="#FFFBEB",
    edgecolor=C_ACCENT_SHM,
    linewidth=1.8,
)
ax.add_patch(rect_shm)

rect_shm_hdr = patches.FancyBboxPatch(
    (4.4, 2.65), 2.8, 0.5,
    boxstyle="round,pad=0.08,rounding_size=0.1",
    facecolor=C_ACCENT_SHM,
    edgecolor=C_ACCENT_SHM,
)
ax.add_patch(rect_shm_hdr)
ax.text(5.8, 2.9, "Segment Pamięci Współdzielonej\n/dev/shm (POSIX Shared Memory)", 
        fontsize=8.5, fontweight="bold", color="#FFFFFF", ha="center", va="center")

shm_text = (
    "• Współdzielony segment RAM hosta\n"
    "• Buforowanie BLOB OOB (Heavy Tail)\n"
    "• Weryfikacja integralności SHA-256\n"
    "• Zero-copy transfer między procesami\n"
    "• Odciążenie kanału JSON-RPC"
)
ax.text(4.55, 2.5, shm_text, fontsize=8, color="#78350F", va="top", linespacing=1.2)

# bramka Middleware: Autorski Serwer FastMCP
rect_mcp = patches.FancyBboxPatch(
    (7.6, 1.0), 2.8, 4.6,
    boxstyle="round,pad=0.15,rounding_size=0.2",
    facecolor=C_CONTAINER_BG,
    edgecolor=C_ACCENT_MCP,
    linewidth=2,
)
ax.add_patch(rect_mcp)

rect_mcp_hdr = patches.FancyBboxPatch(
    (7.6, 5.0), 2.8, 0.6,
    boxstyle="round,pad=0.1,rounding_size=0.1",
    facecolor=C_ACCENT_MCP,
    edgecolor=C_ACCENT_MCP,
)
ax.add_patch(rect_mcp_hdr)
ax.text(9.0, 5.25, "Autorski Serwer MCP\n(FastMCP Gateway Middleware)", 
        fontsize=9.5, fontweight="bold", color="#FFFFFF", ha="center", va="center")

mcp_text = (
    "Zarządzanie zasobami (lifespan):\n"
    "• asyncpg.Pool (min=2, max=10)\n"
    "• ProcessPoolExecutor (AST worker)\n\n"
    "Moduły funkcjonalne:\n"
    "• SchemaEngine (BM25 + pgvector\n"
    "  + RRF + Graph Pruning NetworkX)\n"
    "• Keyset Pagination (HMAC Token,\n"
    "  Welford Adaptive Budget Estimator)\n"
    "• SqlSanitizer (AST sqlglot)\n"
    "• OOB Dispatcher (SHM / HTTP)\n"
    "• KafkaTelemetryConsumer (SSE Push)\n\n"
    "Punkty wejścia:\n"
    "• Transport: stdio / SSE (HTTP)"
)
ax.text(7.75, 4.85, mcp_text, fontsize=8, color="#134E4A", va="top", linespacing=1.2)

# Połączenia strzałkami (Relacje komunikacyjne)
# FastMCP <-> PostgreSQL (asyncpg binary protocol)
arrow_pg = patches.FancyArrowPatch(
    (7.6, 3.2), (4.0, 3.2),
    connectionstyle="arc3,rad=-0.05",
    arrowstyle="<->,head_width=4,head_length=6",
    color=C_ACCENT_PG,
    linewidth=2,
)
ax.add_patch(arrow_pg)
ax.text(5.8, 3.4, "Protokół binarny asyncpg / TCP 5432", fontsize=7.5, fontweight="bold", 
        color=C_ACCENT_PG, ha="center", backgroundcolor="#FFFFFF")

# FastMCP <-> Kafka (aiokafka consumer)
arrow_kf = patches.FancyArrowPatch(
    (7.6, 4.4), (7.2, 4.4),
    arrowstyle="<-,head_width=4,head_length=6",
    color="#E11D48",
    linewidth=1.8,
)
ax.add_patch(arrow_kf)
ax.text(7.4, 4.6, "aiokafka (SSE)", fontsize=7, fontweight="bold", color="#E11D48", ha="center")

# FastMCP <-> /dev/shm (POSIX OOB zero-copy)
arrow_shm = patches.FancyArrowPatch(
    (7.6, 1.8), (7.2, 1.8),
    arrowstyle="<->,head_width=4,head_length=6",
    color=C_ACCENT_SHM,
    linewidth=1.8,
)
ax.add_patch(arrow_shm)
ax.text(7.4, 2.05, "OOB SHM", fontsize=7.5, fontweight="bold", color=C_ACCENT_SHM, ha="center")

# Zewnętrzny Klient / Harness (poza ramką Docker lub na hoście)
plt.tight_layout()

# Zapisanie do wszystkich wymaganych ścieżek
output_paths = [
    Path("doc/figures/architektura_srodowiska.pdf"),
    Path("doc/rysunki/architektura_srodowiska.pdf"),
    Path("doc/EE_dyplom__Copy_/rysunki/architektura_srodowiska.pdf"),
    Path("figures/architektura_srodowiska.pdf"),
]

for p in output_paths:
    p.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(p, format="pdf", bbox_inches="tight")
    print(f"Zapisano diagram architektury: {p}")

plt.close()
