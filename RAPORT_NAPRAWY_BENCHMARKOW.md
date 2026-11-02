# Naprawa aparatury pomiarowej przed nową rundą na RunPod

Gałąź: `fix-benchmarks` (utworzona z `audyt-danych`; zawiera też commity Etapu A i audytu). Nic nie zostało zacommitowane.

Plików `.tex` nie zmieniano. Logika bramy MCP została zachowana: jedyne zmiany w `src/` to instrumentacja czasu w `schema_engine.py` (+17 linii) i `out_of_band.py` (+30 linii); obie są domyślnie wyłączone lub tylko zapisują czasy.

---

## 1. Zmiany w kodzie

### Nowe elementy

| Plik | Co | Dlaczego |
|---|---|---|
| `run_benchmark.sh` | Jedyny punkt wejścia rundy. `set -euo pipefail`; opcje `--quick`, `--only`, `--skip`, `--resume`, `--archive-existing`, `--network-mode`, `--llm-injection`. Log każdego kroku z timestampami w `logs/<scenariusz>.log`. Manifest aktualizowany po każdym kroku; błąd kroku daje kod ≠ 0 i wpis w manifeście. Na końcu `summary.json` i archiwum `benchmarks/results_<run_id>.tar.gz`. | Cz. 1, 9 |
| `benchmarks/run_manager.py` | Archiwizacja, manifest, `environment.json`, kontrola wstępna, kontrola danych, szacowanie czasu, wznowienie. | Cz. 1, 9 |
| `benchmarks/config/benchmark_config.json` | Pełna konfiguracja: ziarna, profile modeli, parametry scenariuszy, nadpisania dla `--quick`. Cała trafia do manifestu. | Cz. 1.6, 3.2 |
| `benchmarks/common/config.py` | Ładowanie konfiguracji. Podstawienie modelu `large` modelem 3B tylko w `quick` (`smoke_test_model_substitution: true`); w `full` przerwanie. | Cz. 3.2, zasada 4 |
| `benchmarks/common/provenance.py` | Manifest (run_id, commit i flaga dirty z `git diff --stat`, konfiguracja, ziarna, czasy scenariuszy, SHA-256 plików). `environment.json`: `nvidia-smi`, `lscpu`, `free -b`, `uname -a`, `nproc`, zmienne RunPod bez sekretów, `pip freeze`, wersje PostgreSQL i rozszerzeń, Kafki, Ollamy, modele z digestami, Docker/NET_ADMIN/tc. | Cz. 1.2–1.3 |
| `benchmarks/common/ollama_client.py` | Jawne `num_ctx`, `temperature`, `seed`, `num_predict`; zapis `prompt_eval_count`, `eval_count`, `total_duration`, `load_duration`, `prompt_eval_duration`, `eval_duration`; flaga `possible_truncation`; tryb strumieniowy z TTFT; sprawdzanie modeli przez `/api/show` z digestami. | Cz. 3.2–3.3, 3.10 |
| `benchmarks/common/ddl_format.py` | Jedna funkcja syntezy DDL dla wszystkich strategii (opisy, wszystkie kolumny z prawdziwymi typami, PK, FK). | Cz. 3.1 |
| `benchmarks/common/metrics_text2sql.py` | Valid SQL, EX i taksonomia błędów zdefiniowane w jednym miejscu (docstring). Asercja: suma kategorii = liczba rekordów z EX = False. | Cz. 3.6 |
| `benchmarks/common/stats.py`, `rules.py`, `db.py` | Mann–Whitney, Wilcoxon, McNemar, bootstrap z ziarnem z konfiguracji; reguła sweet spot; `DISCARD ALL` z logiem. | Cz. 2.3, 3.8, 4.5 |
| `benchmarks/scenarios/text2sql_dataset.py` | Generator zbioru Scenariusza A przeniesiony z poprzedniego skryptu. Sortowana kolejność krawędzi i sąsiadów, ziarno z konfiguracji. **Wcześniej zbiór zależał od `PYTHONHASHSEED` i nie był odtwarzalny.** | Cz. 1.6 |
| `benchmarks/scenarios/bench_ablation_mexp.py` | Ablacja M_exp z czasami RRF, pobrania grafu, pętli mostków i syntezy DDL dla każdego wywołania; sweet spot według reguły. | Cz. 3.9, 2.3 |
| `benchmarks/scenarios/bench_ttft_scaling.py` | Scenariusz `ttft_scaling` (Cz. 3.10): `stream: true`, DDL 1k–28k tokenów, 30 powtórzeń, oba profile. `num_ctx` dopasowane do długości promptu. Cache prefiksów neutralizowany unikalnym nonce na początku promptu (pole `prefix_cache`). | Cz. 3.10 |
| `benchmarks/scenarios/bench_streaming_sse.py` | Nowy Scenariusz C2 opisany w tabeli poniżej. | Cz. 6 |
| `benchmarks/validate_run.py`, `benchmarks/make_summary.py` | `validation_report.md` (kod ≠ 0 przy niezgodności) oraz `summary.json` z pochodzeniem każdej liczby. | Cz. 9.8–9.9 |
| `scripts/versions.env` | Przypięte wersje: PostgreSQL 16.4, pgvector 0.7.4, Kafka 3.7.0, Ollama 0.35.0, modele, obrazy Docker. | Cz. 1.5 |
| `tests/test_benchmark_infrastructure.py`, `tests/test_no_synthetic_data.py` | Testy z Cz. 10. | Cz. 2.4, 10 |

**Scenariusz C2 (`bench_streaming_sse.py`)** składa się z trzech osobnych procesów:

| Proces | Co robi |
|---|---|
| gateway | Prawdziwy `KafkaTelemetryConsumer` bramy; `/latest` do pollingu i `/events` dla SSE. |
| producent | Publikuje zdarzenia w odstępach z rozkładu wykładniczego (średnio 3 s, ziarno z konfiguracji); w treści zdarzenia jest `publish_ts_ns`. |
| klient | Polling według własnego zegara albo subskrypcja SSE. |

Pozostałe cechy C2:
- **Bez losowania fazy.** Opóźnienie pollingu wynika z niezależnych zegarów producenta i klienta.
- **Pasmo jałowe z faktycznych bajtów HTTP** w oknie 60 s; usunięto stałą „+250 B”. Zestawienie połączenia SSE liczone jest osobno (`setup_bytes`).
- **Rzeczywista częstotliwość publikacji** jest zapisywana.

### Zmienione scenariusze

| Plik | Co | Dlaczego |
|---|---|---|
| `benchmarks/scenarios/bench_schema_quality.py` | Przepisany. Selekcja dla k ∈ {3, 5, 10, 20} bez LLM. Generacja tylko dla k = 5 i wariantów przy porównywalnej liczbie tabel (BM25@8, RRF@8 lub `tfinal`); **usunięto kopiowanie wyników k = 5 do innych k**. Pełny zapis per zapytanie: tabele zwrócone i GT, recall, precision, SQL surowy i po ekstrakcji, walidacja, EX (`multiset_unless_order_by` oraz dawna definicja `strict`), kategoria błędu, liczniki tokenów, `possible_truncation`. Domain-Scoped z `ORDER BY` (deterministyczny). Pomiar pełnego zrzutu 1000 tabel w tym samym formacie (`schema_full_dump_tokens.json`). `DISCARD ALL` przed każdą serią. Usunięto zależność od `LLM_MODEL_STRONG` i sekwencje stop `;`/`\n\n`, które ucinały SQL. | Cz. 3 |
| `benchmarks/scenarios/bench_concurrency.py` | Przepisany (Cz. 4): model zamknięty, 30 s pomiaru + 5 s rozgrzewki na poziom (`client_model = closed_loop_asyncio_tasks_single_process`). Warianty SYNC, THREAD, POOL (forkserver) na tej samej funkcji bramy. Siatka workers ∈ {2, 4, 8} przy M = 100 i pełna siatka M dla workers = 4. Każda próbka lagu jest zapisywana. PSS drzewa procesów i liczba procesów potomnych. H3: Mann–Whitney i bootstrapowy CI przy M = 100 i 500. | Cz. 4 |
| `benchmarks/scenarios/bench_oob_crossover.py` | Przepisany (Cz. 5): `measurement_mode ∈ {measured_loopback, netem, emulated}`. Tryb `netem`: rzeczywisty transfer TCP przez `lo` z `tc netem`, wykrywany automatycznie. Tryb `emulated`: dotychczasowy wzór, z parametrami zapisanymi w każdym rekordzie. **Loopback bez sztucznego opóźnienia** (wcześniej In-Band na localhost miał emulowany czas przesyłu). Składowe czasu (Base64, SHA-256, alokacja i odczyt SHM, blob, transfer) oraz `frame_bytes`. S* z przecięcia prostych z bootstrapowym CI; `oob_model_fit.json`; H2: Mann–Whitney i CI. | Cz. 5 |
| `benchmarks/scenarios/bench_security_suite.py` | Wektory w `benchmarks/config/security_vectors.json` bez powielania (**77 unikalnych zamiast 1000**). Nowa kategoria SIDE_EFFECT_SELECT (15 wektorów). Metryka `sanitization_residual_rate` w miejsce ASR. Rozkład narzutu tokenów enkapsulacji XML. Opcjonalny `llm_injection` z transkryptami. Sanitizera nie zmieniano. | Cz. 7 |
| `benchmarks/scenarios/bench_pagination_advanced.py`, `bench_pagination_predicate_ablation.py` | Parametry z konfiguracji (10 powtórzeń + 2 rozgrzewkowe). `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` obu predykatów dla M ∈ {10 000, 250 000, 950 000}. Opis konstrukcji kursora w kodzie (`CURSOR_CONSTRUCTION`) i manifeście. Wyniki spójności i snapshotów zapisywane w `pagination_consistency.json` (wcześniej tylko w logu). | Cz. 8 |
| `benchmarks/scenarios/bench_schema_drift.py` | Parametry z konfiguracji. | Cz. 1.6 |
| `benchmarks/statistical_analysis.py` | Przepisany. **Usunięto wpis H3 ze stałymi** (W = 0, p = 1e-5, r = 0,95). Testy wykonywane tylko na danych; brak danych daje status `not_reported`. | Cz. 2.2 |
| `benchmarks/plot_generators/generate_thesis_plots.py` | Przepisany. Czyta tylko pliki rundy; adnotacje (p, krotności, S*, sweet spot) liczone z danych; przy braku wejścia kończy się kodem ≠ 0. Nowy wykres `fig_ttft_scaling`. Opcja `--from-manifest`: rysuje tylko scenariusze ukończone w manifeście. | Cz. 2.1, 2.3 |
| `testbed/init-scripts/seed_enterprise_tables.py` | `--rows-per-table` (domyślnie z konfiguracji: 200); poprawione domyślne hasło. Krok `data` zasila tabele, jeśli są puste, i przerywa przebieg, jeśli zasilenie się nie udało. | Cz. 3.7 |
| `scripts/setup_runpod.sh` | Wersje z `versions.env`: pakiety PostgreSQL przypięte (`apt-mark hold`, repozytorium PGDG i archiwum PGDG); pgvector kompilowany z tagu; Kafka z przypiętego archiwum; Ollama `OLLAMA_VERSION`; pobranie **obu** modeli; `uv sync --frozen`; `OLLAMA_NUM_PARALLEL=4`; log Ollamy w `/workspace`. | Cz. 1.5, 3.2 |
| `testbed/docker-compose.yml` | Obrazy `pgvector/pgvector:0.7.4-pg16` i `apache/kafka:3.7.0` zamiast `pg16`/`latest`. | Cz. 1.5 |
| `src/mcp_server/schema_engine.py` | `last_graph_timings` (`perf_counter_ns`) w `discover_schema_with_graph_pruning`. | Cz. 3.9 |
| `src/mcp_server/out_of_band.py` | Opcjonalny `TIMING_SINK` ze składowymi czasu (domyślnie `None`). | Cz. 5.4 |

### Usunięte lub wycofane

| Plik | Co |
|---|---|
| `generate_publication_plots.py`, `scripts/generate_schema_discovery_and_pagination_plots.py`, `scripts/generate_streaming_and_security_plots.py` | Usunięte. Generowały dane `np.random` i wpisane adnotacje. |
| `benchmarks/benchmark_streaming.py` z testem | Usunięty. Losował fazę odpytywania; zastąpiony przez C2. |
| `run_benchmarks.sh`, `scripts/run_*.sh`, `scripts/runpod_quickstart.sh`, `benchmarks/run_all_benchmarks.py`, `scripts/preflight_check.py` | Zastąpione komunikatem kierującym do `./run_benchmark.sh` (kod 2). |
| `.gitignore` | Dodano `benchmarks/smoke_results/`, `benchmarks/results_*.tar.gz`, `benchmarks/raw_results_archive/`. |

---

## 2. Testy lokalne

Testy lokalne miały jedynie sprawdzić, że kod się wykonuje i zapisuje pliki o poprawnej strukturze. Każde uruchomienie miało limit 60 s, a wyniki trafiały do `benchmarks/smoke_results/`.

**pytest:** 35 nowych testów przechodzi. Obejmują metryki, taksonomię, S*, testy statystyczne, deduplikację, regułę sweet spot, konfigurację, wykresy bez danych (każdy kończy się błędem), analizę bez danych, transport TCP trybu netem bez `tc` oraz brak losowania w skryptach wykresów i analizy.

Cały zestaw: 129 testów przechodzi, 1 nie. `test_security.py::test_async_validate_concurrent_load` kończy się błędem `CUDA out of memory` na MX450 (1,6 GiB): procesy puli ładują torch na GPU. Uruchomiony osobno przechodzi.

**Scenariusze w trybie `--quick`** (po jednej iteracji, w ramach 60 s):

| Scenariusz | Czas | Co sprawdzono |
|---|---|---|
| pagination | 13 s | wyniki i plany EXPLAIN |
| oob | — | `measured_loopback` i `emulated` (lokalnie brak NET_ADMIN) |
| concurrency | — | 1 poziom M × 3 warianty + siatka workers |
| streaming | 12 s | 1 zdarzenie, SSE i polling 1 s |
| security | — | z `--llm-injection` na 1 wektorze |
| drift | 12 s | |
| schema_quality | 48 s | 1 zapytanie, profil `large` z podstawieniem 3B, strategie Graph i RRF@8 |
| ttft_scaling | 28 s | 1 punkt, profil `large` z podstawieniem |
| analiza statystyczna, wykresy, `make_summary`, `validate_run` | — | na wynikach powyższych scenariuszy |

**Orkiestrator:** `RESULTS_DIR=… ./run_benchmark.sh --quick --only preflight,data,pagination,security` utworzył manifest, `environment.json`, logi z timestampami i archiwum. Sprawdzono też odmowę startu bez `--archive-existing` oraz pomijanie ukończonych kroków przy `--resume`.

Lokalnie zasiliłem tabele schematu po 20 wierszy (`--rows-per-table 20`); wcześniej były puste.

**Czego lokalnie nie uruchomiono:**
- **Pełna runda.** Zgodnie z poleceniem.
- **Profil `large` z prawdziwym modelem 7B** – na RunPod sprawdzą go kontrola wstępna (`/api/show` i digest) oraz pierwszy wpis „Generacja [large]” w `logs/schema_quality.log`.
- **Tryb `netem`** – brak NET_ADMIN. Na RunPod tryb `auto` wybierze `emulated` i zapisze przyczynę w manifeście. Transport TCP sprawdza test jednostkowy.
- **Domyślne konfiguracje skryptów wejściowych `setup_runpod.sh`** – sprawdzono tylko składnię (`bash -n`). Na RunPod potwierdzi je log `setup_runpod.sh` oraz `environment.json` (wersje).

---

## 3. Szacowany czas pełnej rundy na RunPod i wymagania

Szacunki z `run_manager.py estimate`, liczone z konfiguracji i czasów jednostkowych z poprzednich przebiegów:

| Scenariusz | Czas |
|---|---|
| data (zasilenie 1000 tabel po 200 wierszy) | ok. 2 min |
| pagination | ok. 2 min |
| oob | ok. 10 min |
| concurrency (25 poziomów × 37 s) | ok. 16 min |
| streaming (3 warianty × (60 s ciszy + 100 zdarzeń × 3 s)) | ok. 19 min |
| security | ok. 1 min (+ ok. 25 min z `--llm-injection`) |
| drift | ok. 1 min |
| ablation | ok. 2 min |
| ttft_scaling (2 profile × 6 długości × 31 wywołań) | ok. 37 min |
| schema_quality (2 profile × 500 zapytań × 7 generacji) | ok. 4 h |
| statystyka, wykresy, walidacja | ok. 2 min |
| **Razem** | **ok. 5,7 h** (przy GPU klasy A100/A6000) |

Dodatkowo `setup_runpod.sh` zajmie ok. 30–40 min, w tym 1000 tabel z osadzeniami, 1 mln wierszy i pobranie modeli.

**Wymagania:**
- **VRAM:** co najmniej 10 GiB. Kontrola wstępna przerywa poniżej `llm.min_vram_mib_for_large = 10000`. Model 7B (Q4) to ok. 5 GB, plus KV cache dla `num_ctx` 8192 × 4 równoległe żądania. Punkt 28k tokenów w `ttft_scaling` podnosi zapotrzebowanie. **Zalecane 24–48 GB** (A6000, L40S lub A100).
- **Dysk:** modele ok. 7 GB, PostgreSQL ok. 2–3 GB, wyniki ok. 0,5 GB, logi. Kontrola wstępna wymaga ≥ 20 GB wolnego. **Zalecany wolumen ≥ 50 GB.**
- **Trwały wolumen:** repozytorium i `RESULTS_DIR` muszą leżeć na `/workspace`; skrypt ostrzega, gdy tak nie jest. Usługi z `setup_runpod.sh` (PostgreSQL w `/var/lib/postgresql`, Kafka) leżą na dysku kontenera, więc po zatrzymaniu poda trzeba ponownie uruchomić `setup_runpod.sh`. Wyniki w `/workspace` przetrwają, a `--resume` pominie ukończone scenariusze.

---

## 4. Uruchomienie na RunPod krok po kroku

1. **Pod:** szablon z CUDA (Ubuntu 22.04), GPU ≥ 24 GB, wolumen ≥ 50 GB zamontowany w `/workspace`.
2. **Repozytorium na trwałym wolumenie:**
   ```bash
   cd /workspace && git clone <repo> mcp-master-thesis && cd mcp-master-thesis && git checkout fix-benchmarks
   ```
3. **Setup** (instalacja usług w przypiętych wersjach, oba modele, środowisko Python, 1000 tabel i 1 mln wierszy):
   ```bash
   bash scripts/setup_runpod.sh 2>&1 | tee /workspace/setup.log
   ```
4. **Kontrola wstępna i szybki test dymny** (kilka minut, wyniki w `benchmarks/smoke_results/`):
   ```bash
   ./run_benchmark.sh --quick
   ```
   Sprawdź `benchmarks/smoke_results/<znacznik>/validation_report.md` oraz `environment.json` (GPU, wersje, digesty modeli).
5. **Start pełnej rundy w tmux:**
   ```bash
   tmux new -s bench './run_benchmark.sh --archive-existing'
   ```
   Odłączenie: `Ctrl-b d`. `--archive-existing` przenosi stare `benchmarks/raw_results/` do `benchmarks/raw_results_archive/<znacznik>/`.
6. **Postęp:**
   ```bash
   tmux attach -t bench
   ```
   Pomocne są też `tail -f benchmarks/raw_results/logs/<scenariusz>.log` oraz `jq '.scenarios' benchmarks/raw_results/manifest.json`.
7. **Wznowienie po przerwaniu:** jeśli pod był zatrzymany, najpierw ponownie `bash scripts/setup_runpod.sh`, potem:
   ```bash
   tmux new -s bench './run_benchmark.sh --resume'
   ```
8. **Pobranie wyników:**
   ```bash
   scp -P <port> root@<ip>:/workspace/mcp-master-thesis/benchmarks/results_<run_id>.tar.gz .
   ```
   Archiwum zawiera cały katalog wyników z manifestem, środowiskiem, logami, wykresami, `summary.json` i `validation_report.md`. Polecenie jest też wypisywane na końcu przebiegu.

---

## 5. Decyzje pozostawione autorowi

1. **Czy włączyć `llm_injection`** (`--llm-injection`, ok. +25 min). Reguła sukcesu `llm_success_regex` w `security_vectors.json` jest heurystyką słów kluczowych i może zaliczyć jako sukces streszczenie, które tylko cytuje treść ataku; warto ją przejrzeć przed użyciem w pracy.
2. **GPU i VRAM:** czy 28k tokenów w `ttft_scaling` dla 7B (wymaga dużego KV cache), czy ograniczyć cele do 16k.
3. **Definicja EX:** domyślnie `multiset_unless_order_by` (porównanie wartości; kolejność tylko przy ORDER BY w GT). Dawna, ostrzejsza definicja jest zapisywana jako `ex_match_strict`. Którą raportować w pracy.
4. **Budżet tabel:** `equal_budget.mode`: `fixed` (k = 8) albo `tfinal` (k' = |T_final| per zapytanie).
5. **Wersje w `versions.env`:** PostgreSQL 16.4 / pgvector 0.7.4 / Kafka 3.7.0 odpowiadają opisowi w pracy. Wersja Ollamy (0.35.0) jest wzięta z maszyny lokalnej. Jeśli pakietu PostgreSQL 16.4 zabraknie w archiwum PGDG dla danego Ubuntu, setup przerwie się z komunikatem; trzeba wtedy zmienić `PG_MINOR`.
6. **Pamięć puli procesów (C1):** procesy POOL importują cały pakiet bramy (`src/mcp_server/__init__.py` importuje `server`, a z nim torch i model osadzeń). Lokalnie dało to ok. 2,4 GB PSS drzewa procesów zamiast „+8,9 MB” z pracy. Tak działa obecny kod bramy. Zmiana `__init__.py` na leniwy import jest decyzją projektową autora; nie wprowadzałem jej, bo wykracza poza instrumentację.
7. **Liczba wektorów bezpieczeństwa:** po deduplikacji jest 77 unikalnych wektorów. Jeśli praca ma raportować większą próbę, potrzebne są nowe, rzeczywiście różne wektory w `security_vectors.json`.
8. **Scenariusz `snapshot_pool_starvation`:** nie jest w kolejności rundy (praca go nie cytuje); można go dodać do `run_benchmark.sh`.

---

## 6. Miejsca w pracy, których wyniki zmienią się po pełnej rundzie (tylko lista)

- **Streszczenie PL/EN** (`EE_dyplom__Copy_/EE-dyplom.tex`): tokeny, Recall, EX, keyset 0,43 vs 429 ms, 1173 RPS / 0,53 ms (nowy model klientów i warianty), „500 procesów agentowych”, skuteczność mitygacji.
- **1.5 Hipotezy** (`wstep.tex`): H1 (nowy test przy równym budżecie), H2 (Mann–Whitney zamiast Wilcoxona par), H3 (teraz z testem).
- **4.3.4** (`projekt.tex`): latencja kroku grafowego (teraz mierzona osobno).
- **4.4:** parametry modelu progu S*(RTT) (`oob_model_fit.json`).
- **5.x:** opis środowiska i aparatury: `run_benchmark.sh`, manifest, wersje usług.
- **6.1.1–6.1.3** (`wyniki.tex`): definicje Valid SQL i EX, opis środowiska (GPU, wersje, `DISCARD ALL`, ziarna), procedura statystyczna (Mann–Whitney, McNemar, bootstrap).
- **6.2** (Scenariusz A): tabela 7, rys. 2–5, liczby w 6.2.1–6.2.5 (nowy jednolity format DDL, generacja tylko dla k = 5 i równego budżetu, profil `large` = 7B, brak sekwencji stop, zasilone tabele), opis zbioru (teraz w `schema_quality_dataset.jsonl`), pełny zrzut DDL w nowym formacie.
- **6.3** (B1): rys. 6, S* (z CI) i tryb pomiaru LAN/WAN; 6,87× i 97,9 / 673,0 ms (loopback bez emulowanego przesyłu); δ_OOB.
- **6.4.1:** narzut Base64 (z `frame_bytes`).
- **6.4.2:** tabela 8, rys. 7 (10 powtórzeń zamiast 3).
- **6.4.3:** tabela 9 i liczby z EXPLAIN (162 ms, 80 139 / 20 stron).
- **6.4.4:** tabela 10, rys. 8 (warianty SYNC/THREAD/POOL, workers, PSS, mediany i percentyle lagu, adnotacja testu).
- **6.5** (C2): rys. 9 i wszystkie liczby (nowa metoda, interwały 1 s / 5 s, pasmo z bajtów HTTP).
- **6.6** (D): tabela 11 (liczebności unikalnych wektorów, SIDE_EFFECT_SELECT, `sanitization_residual_rate` w miejsce ASR, przedziały Cloppera–Pearsona dla nowych n).
- **6.7.1:** tabela 12. **6.7.2:** nowy rys. TTFT (`fig_ttft_scaling`).
- **6.8.1–6.8.2:** EX, AMR/FPR/ASR w polemice.
- **7.2–7.4** (`podsumowanie.tex`): tabela 13, tabela 14 (w tym narzut XML: lokalnie 31–50 tokenów zamiast „15–25”, latencja grafu, pamięć puli), ograniczenia w 7.4.
