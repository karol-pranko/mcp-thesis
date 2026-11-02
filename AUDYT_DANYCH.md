# Audyt pochodzenia danych w wynikach pracy

Gałąź: `audyt-danych` (bez commitów). Plików .tex, kodu ani danych nie zmieniano.

**Przedmiot audytu:** wersja pracy z HEAD (`e4d92d4`), czyli rysunki 1–10 i tabele 7–14 w numeracji sprzed korekty Etapu A. Niezacommitowane zmiany Etapu A pozostają w drzewie roboczym; kolumna „Po Etapie A” wskazuje, co już skorygowano.

Skrypty audytu są w `benchmarks/audit/`, a ich wyniki w `benchmarks/audit/out/`:

| Skrypt | Zakres |
|---|---|
| `a01_skan_syntetyczne.py` | przeszukanie repozytorium (np.random, random., default_rng, seed, normal/uniform, linspace, adnotacje i p-value wpisane na sztywno) → `a01_trafienia.csv` |
| `a02_pochodzenie_plikow.py` | każdy plik `raw_results/`: mtime, kod zapisujący i czytający, zgodność z kopiami zapasowymi i z jedynym przebiegiem z logami → `a02_pliki.csv` |
| `a03_odtwarzanie_losowych.py` | odtwarzanie plików podejrzanych przez uruchomienie kodu losującego z ziarnami → `a03_odtwarzanie.txt` |
| `a04_porownanie_z_praca.py` | 207 liczb z pracy porównanych z wartościami przeliczonymi z danych → `a04_porownanie.csv` (193 zgodne, 14 niezgodnych lub bez danych) |

Uruchomienie: `.venv/bin/python benchmarks/audit/<skrypt>.py` (a03 trwa ok. 1,5 min, pozostałe kilka sekund).

---

## 0. Ustalenia przekrojowe (Krok 2)

**P1. Dwa różne przebiegi; aktualne dane nie mają logu.**
- Jedyny przebieg z logami to `backup2/benchmarks_results_full.tar.gz`: 2026-10-02 22:54 → 2026-10-03 04:30, pliki `root`, logi w `benchmarks/logs/*_20261002_225433.log`.
- **Wszystkie** pliki Scenariuszy A, B1, C1, drift i statystyki w `raw_results/` różnią się od tego archiwum (a02, kolumna `tar_przebieg_z_logami = rozny`). Pochodzą z innego przebiegu: mtime 2026-10-03 14:28–17:31, bez logu. Przykład: recall przy M_exp = 3 wynosi w archiwum 0,7427, a w `raw_results` 0,5448.
- Format plików zgadza się z kodem zapisującym benchmarków, a wartości mają naturalny rozrzut i nie dają się odtworzyć ziarnem. Te pliki oznaczono jako POMIAR, z adnotacją „brak logu przebiegu”.

**P2. Pliki bez zapisującego kodu w repozytorium.**
- `llm_inference_latencies.csv`: zawartość identyczna z `np.random` (dowód w P3).
- `pagination_predicate_ablation.csv` zapisuje `bench_pagination_predicate_ablation.py` l. 94 (ścieżka podana przez `--output`).
- `pagination_advanced_1m_dnf_baseline_local.csv` nie ma zapisującego kodu; praca z niego nie korzysta.

**P3. Dane syntetyczne potwierdzone odtworzeniem.**
`raw_results/llm_inference_latencies.csv` jest równy co do bitu (max |różnica| = 0,000 we wszystkich 4 kolumnach × 100 próbek) wynikowi `np.random.seed(2026)` + `np.clip(np.random.normal(...))`. Źródło: `benchmarks/plot_generators/generate_publication_plots.py` l. 1121–1131. Ślad przycięcia: 2 wartości TTFT równe dokładnie granicy 25,0. Ten sam model (seed 42) jest w `scripts/generate_schema_discovery_and_pagination_plots.py` l. 250–267.

**P4. Statystyka wpisana na sztywno.**
`benchmarks/statistical_analysis.py` l. 432–439 zapisuje hipotezę H3 ze stałymi `sample_size=100, statistic_w=0.0, p_value=1e-5, effect_size_r=0.95`, bez wykonania testu. Ta wartość trafia na rys. 8 jako adnotacja „Wilcoxon: p = 1,00·10⁻⁵” (`generate_thesis_plots.py`, HEAD l. 724). Praca w 6.7.1 sama stwierdza, że dla H3 testu nie wykonano.

**P5. Adnotacje na rysunkach wpisane na sztywno.**

| Rysunek | Adnotacja | Skąd | Wartość z danych |
|---|---|---|---|
| Rys. 9 | „709×”, „4165×” | `generate_publication_plots.py` l. 910 | 702,5× i 4692,1× (z danych rysunku) |
| Rys. 2 | „p = 5,53·10⁻⁴², r = 1,000” | HEAD `generate_thesis_plots.py` l. 267 | zgodna z danymi, ale to wynik konstrukcyjny |
| Rys. 5 | „Sweet Spot: M_exp = 2” | `sweet_m = 2` na sztywno | praca używa M_exp = 3 |

**P6. Składniki modelowane, a nie mierzone.**
- **OOB (B1):** czas sieci to `inject_network_delay(RTT + rozmiar·8/przepustowość)`, czyli sleep wyliczony ze wzoru z parametrami autora (`bench_oob_crossover.py` l. 81–117, 196–270). Dotyczy profili LAN/WAN oraz ścieżki In-Band na localhost (10 Gb/s). Mierzony jest tylko składnik CPU (Base64, SHA-256, IPC). Progi S\*_LAN i S\*_WAN wynikają więc w dużej mierze z przyjętych parametrów.
- **REST Polling (C2):** opóźnienie detekcji to w praktyce `random.uniform(0.01, interwał)` (losowe przesunięcie fazy) plus krótki czas odpytania (`benchmark_streaming.py` l. 337–347). Ziarno nie jest ustawione, więc dokładne odtworzenie jest niemożliwe. Rozkład próbek z rys. 9 zgadza się z tym modelem (100% próbek w [10 ms, interwał+50 ms]). Mierzony jest tylko SSE.
- **Pasmo jałowe (C2):** to `len(JSON) + 250 B` przyjęte jako narzut nagłówków (`benchmark_streaming.py` l. 242–243), a nie bajty przechwycone w sieci.

**P7. Rys. 9 i tekst 6.5 pochodzą z różnych przebiegów.**

| Źródło | Plik | Mtime | N | Interwały |
|---|---|---|---|---|
| Rys. 9 | `results-remote-gpu/streaming_comparison.json` | 2026-10-01 18:57 | 100 | 1000 / 5000 ms |
| Tekst 6.5, tab. 14 | `raw_results/streaming_comparison.*` | 2026-10-01 16:44 | **2** | **100 / 500 ms** |

Tekst opisuje dane z `raw_results` jako „interwał 1 s / 5 s”.

**P8. Logi systemowe (Krok 2.4).**
- Lokalna Ollama (`journalctl -u ollama`): 205 wywołań `/api/generate` w dniach 1–2.10, wyłącznie `qwen2.5-coder:3b`, GPU GeForce MX450 (1,6 GiB), domyślne `num_ctx = 4096`. Lokalnie nie wykonano więc 5000 wywołań Scenariusza A.
- Kontenery lokalne: PostgreSQL 16.15, pgvector 0.8.6, Kafka 4.3.1.
- Lokalnie nie ma `~/.ollama/logs`, logów RunPod ani logów Dockera z przebiegu zdalnego.
- Log przebiegu zdalnego (`bench_schema_quality_20261002_225433.log`) pokazuje **oba profile z `qwen2.5-coder:3b`**. Zmiennej `LLM_MODEL_STRONG` nie ustawia żaden skrypt.
- `qwen2.5-coder:3b` ma okno kontekstu 32 768 tokenów (`ollama show`), więc prompt „pełnego zrzutu” o 100 555 tokenach nie mieściłby się w nim.

**P9. Sprawdzone, bez zastrzeżeń:**
- strumienie z `results-remote-gpu` nie są odtwarzalne `np.random` (ziarna 0–199 999, a03);
- brak śladów `np.clip` w pozostałych plikach `raw_results` (jedyne trafienia to zaokrąglenie przepustowości 0,01 MB/s i duplikaty bootstrapu dla Baseline A);
- H1, H1b i H2 w `statistical_hypothesis_tests.csv` policzono prawdziwym testem (`calculate_wilcoxon_test`);
- pliki `concurrency_*`, `oob_*`, `pagination_*`, `security_suite_*` i `schema_*` zapisuje kod scenariuszy, który faktycznie wykonuje SQL lub kod i mierzy czas `perf_counter`.

---

## 1. Tabela elementów pracy

Kategorie: **POMIAR**, **POMIAR-NIESPÓJNY**, **SYNTETYCZNE**, **NIEUSTALONE**. Strony według PDF z HEAD.

| Element | Strona / podrozdział | Skrypt | Plik danych | Kategoria | Dowód | Po Etapie A |
|---|---|---|---|---|---|---|
| Rys. 1 (architektura) | s. 31, 4.1 | `scripts/generate_architecture_diagram.py` | – | n/d (diagram) | brak danych liczbowych | – |
| Tab. 7 | s. 54, 6.2.1 | ręcznie w .tex | `schema_quality_results.parquet` (k=5, profil B) | POMIAR (brak logu, P1) | a04: 30/30 wartości zgodnych. „Tokeny DDL” to w danych `prompt_tokens` (cały prompt); generacja LLM tylko dla k=5 | nagłówek poprawiony |
| Rys. 2 | s. 55, 6.2.2 | `generate_thesis_plots.py::generate_schema_recall_precision` | `schema_quality_n1000_dual_engine.parquet` | POMIAR; adnotacja POMIAR-NIESPÓJNY | recall/precision zgodne (a04, 12/12). Adnotacja „p = 5,53·10⁻⁴²” wpisana na sztywno; test konstrukcyjny. Etykieta „Full Schema” = 15 tabel domeny | przerysowany |
| Rys. 3 | s. 56, 6.2.3 | `…::generate_text2sql_accuracy` | `schema_quality_summary.csv` | POMIAR-NIESPÓJNY | wartości zgodne (a04). Oś „zaakceptowane przez EXPLAIN” bez „i wykonane”. Etykieta „Full Schema” błędna. Model 7B w profilu B niepotwierdzony (P8) | przerysowany |
| Rys. 4 | s. 57, 6.2.4 | `…::generate_error_decomposition_tree` | `schema_error_decomposition.csv` | POMIAR-NIESPÓJNY | agregat 5 strategii z 4-krotnymi kopiami k; tekst podaje wartości dla wariantu autorskiego. „200” i „508 przypadków” = 4×50 i 4×127 (a04) | przerysowany |
| Rys. 5 | s. 58, 6.2.5 | `…::generate_ablation_mexp` | `ablation_m_exp_results.csv` | POMIAR-NIESPÓJNY | wartości zgodne (a04, 9/9). Adnotacja „Sweet Spot: M_exp = 2” wpisana na sztywno. Podpis „latencja grafowa” – brak na wykresie. 1622,5 vs 493,7 to różny format DDL | przerysowany |
| Rys. 6 | s. 59, 6.3 | `…::generate_oob_crossover_rtt` | `oob_crossover_*.csv/json` | POMIAR; LAN/WAN częściowo SYNTETYCZNE (P6) | S\* i czasy zgodne (a04, 9/9). Sieć emulowana sleepem ze wzoru o parametrach RTT 1/30 ms, 1000/250 Mb/s | – |
| Tab. 8 | s. 62, 6.4.2 | ręcznie w .tex | `pagination_advanced_summary.csv` | POMIAR (brak logu) | 25/25 median zgodnych; regresja 44,8 ms/100 tys., R² = 0,998, 991× zgodne. Plik z 2026-10-04 09:26 bez logu; logowany przebieg 09:04 daje 137,4 ms przy 950 tys. (nie 429) | – |
| Rys. 7 | s. 61, 6.4.2 | `…::generate_pagination_depth_1m` | `pagination_advanced_1m.csv` | POMIAR (brak logu) | jw.; „991×” liczone z danych | – |
| Tab. 9 | s. 63, 6.4.3 | ręcznie w .tex | `pagination_predicate_ablation.csv` | POMIAR; liczby z EXPLAIN NIEUSTALONE | 10/10 median, 10 powtórzeń i zakres 0,17–0,50 zgodne. Środowisko lokalne (8 rdzeni, 15 GB, PG 16.15) potwierdzone. Liczby z EXPLAIN (162 ms, 80 139 / 20 stron bufora, 0,6 ms) – brak pliku z planem | – |
| Tab. 10 | s. 64, 6.4.4 | ręcznie w .tex | `concurrency_summary.csv` | POMIAR (brak logu) | 35/35 wartości zgodnych; +308,9% i 8,9 MB zgodne. Lag SYNC to 1 obserwacja (mean = p95 = max) | – |
| Rys. 8 | s. 64, 6.4.4 | `…::generate_concurrency_rps_lag` | `concurrency_summary.csv` + `statistical_hypothesis_tests.csv` | POMIAR; adnotacja SYNTETYCZNE | adnotacja „Wilcoxon p = 1,00·10⁻⁵” to wartość wpisana na sztywno (P4) | bez zmian |
| Rys. 9 | s. 65, 6.5 | `generate_publication_plots.py::generate_streaming_sse_vs_polling` | `results-remote-gpu/streaming_comparison.json` | SSE: POMIAR; polling: SYNTETYCZNE (model fazy); adnotacje: SYNTETYCZNE | P6, P7; „709×/4165×” wpisane na sztywno (dane: 702,5× / 4692,1×); pasmo z założonych 250 B | bez zmian |
| Tekst 6.5 | s. 65–66 | – | `raw_results/streaming_comparison.csv` | POMIAR-NIESPÓJNY | N = 2 próbki; interwały 100/500 ms opisane jako 1 s / 5 s; inny przebieg niż rys. 9 | bez zmian |
| Tab. 11 | s. 66, 6.6 | ręcznie w .tex | `security_suite_*` | AMR/FPR i czasy: POMIAR; ASR: POMIAR-NIESPÓJNY | 11/11 wartości (w tym przedziały Cloppera–Pearsona i p95 = 3,46 ms) zgodne. ASR mierzy obecność surowych tokenów (`<\|im_start\|>`, `[INST]`, `system:`…) po enkapsulacji, bez modelu językowego; praca opisuje „udane przejęcia kontekstu agenta”. Wektory to 8 szablonów powielonych do 125 | bez zmian |
| Tab. 12 | s. 67, 6.7.1 | ręcznie w .tex | `statistical_hypothesis_tests.csv` | H1, H1b: POMIAR (test konstrukcyjny); H2: POMIAR (częściowo emulowany wire-time) | p zgodne; H1 W = 0 wynika z T_final ⊇ RRF@5 | H1/H1b przebudowane |
| Rys. 10 | s. 68, 6.7.2 | `generate_thesis_plots.py::generate_ttft_le2e_boxplots` | `llm_inference_latencies.csv` | **SYNTETYCZNE** | P3; adnotacja „W = 0, p < 10⁻¹⁵, r = 1,00” wpisana na sztywno; baseline 100 555 tokenów przekracza okno 32 768 modelu (P8) | usunięty |
| Tab. 13 | s. 73, 7.2 | ręcznie w .tex | różne | mieszana | zob. sekcja 2 | H1/H1b/PB1 skorygowane |
| Tab. 14 | s. 75, 7.3 | ręcznie w .tex | różne | mieszana | „≈ 2,1 ms, do 4,9 ms” i „≈ 15–25 tokenów na ramkę”: NIEUSTALONE; SSE 1,13 vs 66–126 ms: POMIAR-NIESPÓJNY (N=2, 100/500 ms); reszta POMIAR | JIT skorygowany |
| 6.1.2 (środowisko) | s. 51 | – | – | NIEUSTALONE / NIESPÓJNE | A100, EPYC 7742, 250 GB, PG 16.2, pgvector 0.7.0, Docker 26.0 – brak logów; Kafka 3.7.0 zgodna z `setup_runpod.sh` l. 186; `statement_timeout = 10000 ms` zgodne z `setup_runpod.sh`; **`DISCARD ALL` – brak w kodzie**; **`seed = 42` dla LLM – brak w `agent_loop.py`** (opcje: tylko `temperature`, `num_predict`, `stop`); warmup 5 i próbkowanie 10 ms zgodne z kodem | – |
| 6.4.1 (Base64 33,33%) | s. 60 | – | `heavy_tail_comparison.csv` (64 KB–4 MB) | POMIAR | 5 592 630 / 4 194 304 − 1 = 33,34% | – |
| 6.4.3 (spójność 10 000 rekordów) | s. 63 | `bench_pagination_advanced.py` | log 2026-10-03 02:20 | POMIAR | log: „Dokładna zgodność sekwencji z GT: TAK (100%)”, 0 duplikatów, brak phantom reads | – |
| 6.2.5 / 4.3.4 latencja grafu | s. 58, 4.3.4 | – | `ablation_m_exp_results.csv` | 20,6 ms: POMIAR; „< 2 ms”, „2,1 ms”, „4,9 ms”: NIEUSTALONE | dane: średnie 20,6–22,0 ms całego wywołania; brak pomiaru samego kroku grafowego | skorygowane |
| 7.4 p. 2 (drift) | s. 75–76 | `bench_schema_drift.py` | `schema_drift_summary.json` | POMIAR (brak logu) | 8/8 wartości zgodnych | – |
| 6.8.1 | s. 69 | – | `schema_quality_results` | POMIAR-NIESPÓJNY | liczby zgodne; „redukcja TABLE_MISSING 27,0 → 11,9%” pomija RRF (4,4%) i BM25 (1,9%) | skorygowane |

---

## 2. Liczby ze streszczeń PL/EN i z tabeli 13 (wersja HEAD)

### Streszczenie PL i EN (te same liczby)

| Liczba | Kategoria | Uwagi |
|---|---|---|
| N = 500 zapytań Text-to-SQL | POMIAR | 500 `query_id` w danych |
| 1000 relacji | POMIAR | `schema_drift_summary`: nodes_before = 1000, edges = 1174 |
| 97,8% redukcji tokenów DDL | POMIAR-NIESPÓJNY | 1 − 2218,8 / 100 555 = 97,79%. Porównuje dwa pomiary w różnych formatach; pełnego zrzutu nie ewaluowano |
| ok. 100,6 tys. tokenów | POMIAR | `schema_scaling_analysis.csv` (2026-10-01, `benchmark_schema.py`) |
| 2219 tokenów | POMIAR | średni cały prompt, nie sam DDL |
| Table-Recall@5 = 54,5% | POMIAR | brak logu (P1) |
| 79,3% dla k = 20 | POMIAR | |
| TTFT −97,5% | **SYNTETYCZNE** | P3 |
| 1382,7 ms → 34,7 ms (EN: 1382.7 → 34.7) | **SYNTETYCZNE** | P3 |
| O(log N + K) do 95% tabeli | POMIAR | brak logu przebiegu 09:26 |
| 0,43 ms przy offsecie 950 000 | POMIAR | |
| 429 ms dla OFFSET/LIMIT | POMIAR | logowany przebieg z 09:04 dał 137,4 ms |
| 1173 RPS | POMIAR | |
| < 1 ms (0,53 ms) | POMIAR | średnia, nie mediana |
| 500 współbieżnych procesów agentowych (EN: concurrent agent clients) | POMIAR-NIESPÓJNY | to 500 zadań `asyncio` w jednym procesie (`bench_concurrency.py` l. 247–264) |
| „wysoka skuteczność mitygacji” / „no successful structural injection attacks” | POMIAR | AMR 100% na 250 wektorach |

### Tabela 13

| Wiersz | Liczba | Kategoria |
|---|---|---|
| H1 | 54,5% vs 39,2% | POMIAR |
| | mediany 0,50 vs 0,33 | POMIAR |
| | N = 500, n_eff = 243, W = 0, p = 5,53·10⁻⁴², r = 1,000 | POMIAR (wynik konstrukcyjny) |
| H1b | mediany 0,60 vs 0,50, n_eff = 243, p = 9,91·10⁻⁴², r = 1,000 | POMIAR (konstrukcyjny; kolumny wynikają z tabel) |
| H2 | 127,6 → 15,5 ms | POMIAR (In-Band zawiera emulowany czas przesyłu przy 10 Gb/s) |
| | −87,9% | POMIAR |
| | N = 50, p = 1,78·10⁻¹⁵ | POMIAR (minimalne dokładne p dla 50 par) |
| | r = 1,000 | POMIAR |
| H3 | „mediana” 0,53 vs 3357,2 ms | POMIAR-NIESPÓJNY (to średnie; SYNC = 1 obserwacja) |
| | 8702,7 ms przy M = 500 | POMIAR |
| PB1 | 39,2 → 54,5%, 79,3% | POMIAR |
| | TABLE_MISSING 27,0 → 11,9% | POMIAR-NIESPÓJNY (porównanie tylko z Baseline A; RRF 4,4%, BM25 1,9%) |
| | „narzut DDL poniżej 2200” | POMIAR-NIESPÓJNY (średni prompt 2218,8; sam DDL 1998,4) |
| | ok. 100,6 tys. tokenów | POMIAR |
| PB2 | S\* = 5,19 kB | POMIAR |
| | LAN 122,92 kB, WAN 1,68 MB | POMIAR częściowo SYNTETYCZNE (sieć ze wzoru, P6) |
| | 6,87×; 673,0 → 97,9 ms | POMIAR |
| PB3 | 0,53 vs 8702,7 ms | POMIAR |
| | 1173,0 RPS, +308,9% | POMIAR |
| | AMR 100%, FPR 0% | POMIAR |
| | ASR 0% | POMIAR-NIESPÓJNY (inna wielkość niż opisana, P5/tab. 11) |
| Paginacja | 0,43 vs 429 ms, 991× | POMIAR |

---

## 3. Elementy SYNTETYCZNE i NIEUSTALONE: co uruchomić i ile to potrwa

Szacunki czasu oparto na kodzie (liczba iteracji, timeouty) i na czasach zapisanych w danych.

| Element | Scenariusz | Skrypt do uruchomienia | Szacowany czas |
|---|---|---|---|
| Rys. 10, TTFT/L_E2E (streszczenie, 6.7.2, 7.2) | Scenariusz A (inferencja) | **Brak skryptu.** `agent_loop.call_ollama` wysyła `stream: False`, więc nie mierzy TTFT. Potrzebny nowy scenariusz ze `stream: True` (czas pierwszego fragmentu) i jawnym `num_ctx`. Wariant „pełny zrzut” (100 555 tokenów) nie zmieści się w oknie 32 768 tokenów `qwen2.5-coder`; realny baseline to 15 tabel domeny lub model z oknem ≥ 128k | 2 warianty × 100 próbek. Przy medianach `latency_ms` z danych (12,7–19,1 s na wywołanie) ok. **40–65 min** sekwencyjnie |
| Rys. 9 i 6.5: polling (model fazy) i tekst z N = 2 | C2 | `benchmarks/benchmark_streaming.py` z domyślnymi `iterations=100`, `warmup_runs=5`, interwałami 1,0 / 5,0 s, idle 3 s i Kafką. Polling z natury modeluje fazę; prawdziwy pomiar wymagałby klienta odpytującego niezależnie od chwili publikacji (bez `random.uniform`) | SSE ~105 × ~1 ms; polling 1 s ~105 × 0,5 s ≈ 55 s; polling 5 s ~105 × 2,5 s ≈ 4,5 min; idle 3 × 3 s. Razem ok. **6 min** |
| Rys. 8: adnotacja Wilcoxona (H3) | C1 | `bench_concurrency.py`, a następnie test na próbkach lagu z `EventLoopLagProbe` (10 ms) zamiast stałych w `statistical_analysis.py`. W SYNC sonda rejestruje 1 obserwację na poziom, więc test par wymaga zmiany projektu pomiaru | 7 poziomów × 2 warianty × 1000–2500 żądań przy 82–1178 RPS: ok. **1–2 min** (w logu 2026-10-03: 04:28 → 04:29) |
| Rys. 9: adnotacje 709× / 4165× | C2 | wystarczy wyliczenie z danych | < 1 min |
| LAN/WAN S\* (rys. 6, 6.3, tab. 13–14) | B1 | `bench_oob_crossover.py` daje te same wartości modelowe. Prawdziwy pomiar wymaga dwóch hostów lub `tc netem` zamiast `inject_network_delay` | 3 profile × 9 rozmiarów × 3 tryby × 50 powtórzeń; dominuje WAN (~60 ms × 1350 prób ≈ 80 s) i 50 MB In-Band (~0,7 s × 150 ≈ 105 s). Razem ok. **3–5 min** (log 2026-10-03: ok. 8 min z warmupem) |
| Środowisko 6.1.2 (A100, EPYC, PG 16.2, pgvector 0.7.0, Docker 26.0, `DISCARD ALL`, `seed=42`) | wszystkie | bez ponownego pomiaru: zapis `nvidia-smi`, `lscpu`, `SELECT version()`, wersji rozszerzeń i obrazów przy kolejnym przebiegu. `DISCARD ALL` i `seed` trzeba zaimplementować albo usunąć z tekstu | minuty |
| Profil B = 7B (6.2.3) | A | `bench_schema_quality.py` z `LLM_MODEL_STRONG=qwen2.5-coder:7b` i zapisem logu | ok. **3 h** (mtime 14:28 → 17:21) do **5,5 h** (log 22:54 → 04:20) dla 2 profili |
| Latencja kroku grafowego „< 2 ms / 2,1 ms / 4,9 ms” (4.3.4, tab. 14) | A (ablacja) | `AblationStudyRunner` z per-zapytaniowym zapisem czasu i osobnym pomiarem kroku grafowego (bez RRF i DDL) | 5 × 500 wywołań × ok. 21 ms ≈ **1 min** + ładowanie modelu osadzeń |
| Liczby z EXPLAIN w 6.4.3 (162 ms, 80 139 / 20 stron bufora, 0,6 ms) | B2 | `EXPLAIN (ANALYZE, BUFFERS)` dla 2 postaci predykatu przy M = 950 000, z zapisem planu | **< 1 min** |
| „≈ 15–25 tokenów na ramkę XML” (tab. 14) | D | brak skryptu; zliczenie tokenów `encapsulate_untrusted_database_records` dla przykładowych rekordów | **< 1 min** |
| „50 zdarzeń/s” (6.5) | C2 | `kafka_stream_producer.py --rate 50` z zapisem w logu | jak C2 |

---

## 4. Scenariusze bez danych pomiarowych

1. **Opóźnienia wnioskowania LLM (TTFT, L_E2E; 6.7.2, rys. 10, streszczenia, tab. 13 / 7.2).** W repozytorium nie ma żadnego pomiaru; jedyny plik jest syntetyczny (P3), a kod klienta LLM nie umożliwia pomiaru TTFT.
2. **Test statystyczny H3 (rys. 8).** Statystyka jest wpisana na sztywno (P4); pomiar lagu istnieje, ale testu nie wykonano.
3. **REST Polling TTFE (C2).** Wartość wynika z losowanego przesunięcia fazy (P6), a nie z niezależnego pomiaru. Pasmo jałowe jest liczone z założonego narzutu 250 B.
4. **Opóźnienie sieci w profilach LAN/WAN (B1).** Emulowane sleepem ze wzoru; pomiaru sieciowego brak.
5. **Narzut tokenowy enkapsulacji XML (tab. 14)** – brak danych.
6. **Czas samego kroku analizy grafowej (4.3.4, tab. 14)** – brak danych; zmierzono tylko całe wywołanie.
7. **Plany EXPLAIN (6.4.3)** – nie zapisano.
8. **Parametry środowiska z 6.1.2** – brak zapisu dla przebiegu źródłowego.
9. **Przebiegi źródłowe Scenariuszy A, B1, B2, C1, D i drift (P1)** – dane istnieją, ale bez logów. Jedyny logowany przebieg (archiwum `backup2`) daje inne wartości, a dla paginacji logowany przebieg z 2026-10-04 09:04 też się różni (137 vs 429 ms). Pomiary są wiarygodne co do mechanizmu, ale ich przebiegu nie da się powiązać z konkretnym środowiskiem.
