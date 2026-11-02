# Analiza wydajności i bezpieczeństwa protokołu MCP w integracji systemów agentowych z wielkimi zbiorami danych

Repozytorium towarzyszące pracy magisterskiej Karola Sygieta. Zawiera kod źródłowy referencyjnej bramy pośredniczącej w standardzie Model Context Protocol (MCP), środowisko badawcze, aparaturę pomiarową oraz pliki źródłowe tekstu pracy w systemie LaTeX.

Brama udostępnia agentom opartym na modelach językowych dostęp do bazy PostgreSQL o złożonym schemacie (1000 tabel, 1174 klucze obce). Składa się z następujących modułów:

- selekcja schematu łącząca wyszukiwanie leksykalne (BM25) i wektorowe (pgvector, fuzja RRF) z ekspansją grafową o tabele mostkujące,
- wielokolumnowa paginacja kursorowa keyset z kontrolą budżetu tokenów,
- transfer danych binarnych poza ramką JSON-RPC (Out-of-Band) wraz z analitycznym modelem progu opłacalności,
- zapora zapytań SQL oparta na analizie drzew AST, wykonywana w puli procesów, aby nie blokować pętli zdarzeń.

## Struktura repozytorium

| Ścieżka | Zawartość |
|---|---|
| `src/mcp_server/` | brama MCP (FastMCP): `server.py`, `schema_engine.py`, `pagination.py`, `out_of_band.py`, `security.py`, `streaming.py` |
| `src/agent_client/` | klient agentowy ReAct i klient MCP używany w pomiarach |
| `testbed/` | konfiguracja deweloperska Docker Compose (PostgreSQL 16 + pgvector, Apache Kafka KRaft) i generatory danych |
| `benchmarks/` | scenariusze pomiarowe, konfiguracja rundy (`config/benchmark_config.json`), analizy i generatory wykresów |
| `tests/` | testy `pytest` |
| `scripts/` | przygotowanie środowiska GPU (`setup_runpod.sh`), kontrola gotowości, przypięte wersje usług (`versions.env`) |
| `doc/` | tekst pracy (LaTeX, klasa `EE-dyplom`) |
| `PROMPTS.txt` | prompty eksploracyjne użyte przy tworzeniu projektu (plik na razie pusty, będzie uzupełniany) |

## Uruchomienie

Środowisko deweloperskie (Python 3.12, [uv](https://docs.astral.sh/uv/), Docker):

```bash
uv sync
docker compose -f testbed/docker-compose.yml up -d
uv run pytest
```

Pełna runda pomiarowa (Linux z GPU NVIDIA; usługi są instalowane natywnie, a modele `qwen2.5-coder:3b` i `qwen2.5-coder:7b` uruchamiane przez Ollamę):

```bash
bash scripts/setup_runpod.sh
./run_benchmark.sh --archive-existing --llm-injection
```

Test dymny, czyli jedna iteracja każdego scenariusza, uruchamia się poleceniem `./run_benchmark.sh --quick`. Liczby i tabele pracy odtwarzają skrypty z katalogu `benchmarks/analysis_final/` (`analiza.py`, `generuj_makra.py`). Szczegółowy opis środowiska i procedury znajduje się w rozdziale 5 i załączniku 1 pracy.
