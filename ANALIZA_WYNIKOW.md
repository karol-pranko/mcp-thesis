# Analiza wyników rundy `20261004T120209_d347d2` (Fazy 1–2)

Źródło: wyłącznie `results-remote-new-sunday/`. Wszystkie liczby obliczył `benchmarks/analysis_final/analiza.py`
(→ `benchmarks/analysis_final/out/wyniki.json`, 362 wartości z polami `src` i `how`). Ziarno bootstrapu 12345 (z manifestu).

## 1. Inwentaryzacja rundy

| Element | Stan |
|---|---|
| Tryb | `full`, `smoke_test_model_substitution = false` |
| Kod | commit `774b75a`, `dirty = true`: jedyna różnica to poprawka `benchmarks/common/db.py` (`reload_schema_state()` po `DISCARD ALL`, +1 linia). Ta poprawka nie jest zacommitowana. |
| Sumy SHA-256 | 84/84 plików zgodnych z manifestem |
| Wznowienia | jedno wznowienie (12:51) |
| Scenariusze ukończone | preflight, data, pagination, oob, concurrency, security (+ llm_injection), schema_quality, ablation, ttft_scaling, drift, statistics, plots |
| Scenariusze nieudane | **streaming (C2)**, exit 1. **validation** nie przeszła wyłącznie z powodu C2. |
| Uwagi scenariuszy | oob: `network_mode = emulated` dla LAN/WAN (brak NET_ADMIN); security: `llm_injection = true`, uruchomione flagą, choć w konfiguracji jest `false` |
| Środowisko (`environment.json`) | NVIDIA A100-SXM4-80GB; AMD EPYC 7742 (przydział RunPod: 16 vCPU, 250 GB); jądro 6.8.0-100-generic; PostgreSQL 16.4; pgvector 0.7.4; Kafka 3.7.0; Ollama 0.35.0; Python 3.12.3; bez Dockera; `statement_timeout` 10000 ms; `shared_buffers` 2621448 kB; `work_mem` 65536 kB |
| Modele | qwen2.5-coder:3b `f72c60c…`, qwen2.5-coder:7b `dae161e…`; `num_ctx` 8192, T = 0, `num_predict` 1024 |

**Przyczyna awarii C2** (`logs/streaming.log`): warianty SSE i POLL_1S wykryły 100/100 zdarzeń. Klient POLL_5S przerwał pracę o 12:40:52 z wyjątkiem `httpx.RemoteProtocolError: Server disconnected without sending a response`. Najbardziej prawdopodobna przyczyna to wyścig keep-alive: domyślny `timeout_keep_alive` uvicorn (5 s) jest równy interwałowi odpytywania (5 s). Plik `streaming_events.csv` nie powstał. Częściowe pliki w `_streaming_tmp/` **nie zostały użyte**.

## 2. Wyniki liczbowe

### A. Zbiór danych i kontekst
- N = 500 zapytań: 150 prostych, 250 średnich, 100 złożonych.
- Liczba tabel GT: 2 → 75, 3 → 200, 4 → 160, 5 → 35, 6 → 30. ≥ 3 tabele w 85,0% zapytań.
- Ekspansja dodała ≥ 1 tabelę GT w 241 zapytaniach (48,2%). Graph@5 zwraca 5–8 tabel (średnio 6,02).
- Pełny zrzut DDL (ten sam format, 1000 tabel): 263 113 tokenów. Graph@5 DDL: 1482,4 tokenu, czyli redukcja o 99,44%.
- `possible_truncation` = 0. Maksymalny `prompt_eval_count` = 3691 < `num_ctx` 8192.
- Taksonomia błędów spójna: suma kategorii równa liczbie rekordów z EX = False.

### A. Selekcja (średnie, N = 500)

| Strategia | Tabel | T-Recall | T-Prec | C-Recall | Tokeny DDL | Tokeny promptu |
|---|---|---|---|---|---|---|
| DomainScoped(15) | 15,00 | 8,5 | 2,0 | 8,2 | 3264 | 3484 |
| BM25@5 | 5,00 | 47,8 | 32,1 | 62,4 | 1147 | 1367 |
| Dense@5 | 5,00 | 31,2 | 19,8 | 40,9 | 1166 | 1387 |
| RRF@5 | 5,00 | 40,4 | 26,5 | 53,3 | 1161 | 1382 |
| Graph@5 | 6,02 | 55,3 | 30,4 | 62,5 | 1482 | 1703 |
| BM25@8 | 8,00 | 53,2 | 22,6 | 67,2 | 1848 | 2068 |
| RRF@8 | 8,00 | 47,0 | 19,6 | 60,2 | 1870 | 2090 |

Table-Recall Graph przy k = 3/5/10/20: 44,7 / 55,3 / 67,4 / 76,2. Przy k = 10: BM25 55,1, RRF 49,6.

### A. Text-to-SQL (%, 95% CI Cloppera–Pearsona)

| Strategia | 3B Valid | 3B EX [CI] | 7B Valid | 7B EX [CI] |
|---|---|---|---|---|
| DomainScoped | 9,4 | 7,8 [5,6; 10,5] | 15,2 | 6,6 [4,6; 9,1] |
| BM25@5 | 18,6 | 15,4 [12,3; 18,9] | 21,8 | 15,8 [12,7; 19,3] |
| Dense@5 | 19,6 | 16,0 [12,9; 19,5] | 18,4 | 14,0 [11,1; 17,4] |
| RRF@5 | 20,0 | 15,8 [12,7; 19,3] | 22,2 | 15,0 [12,0; 18,4] |
| Graph@5 | 20,8 | 17,0 [13,8; 20,6] | 22,8 | 15,2 [12,2; 18,7] |
| BM25@8 | 20,4 | 16,6 [13,4; 20,2] | 24,0 | 17,8 [14,5; 21,4] |
| RRF@8 | 18,8 | 15,8 [12,7; 19,3] | 21,8 | 15,2 [12,2; 18,7] |

McNemar, Graph@5 vs baseline: pary (tylko Graph / tylko baseline), p.

| Baseline | 7B | 3B |
|---|---|---|
| BM25@8 | 9/22, **p = 0,029** (BM25@8 lepszy) | 10/8, p = 0,81 |
| RRF@8 | 14/14, p = 1,0 | 13/7, p = 0,26 |
| RRF@5 | 7/6, p = 1,0 | 9/3, p = 0,146 |
| BM25@5 | 14/17, p = 0,72 | 16/8, p = 0,15 |
| Dense@5 | 15/9, p = 0,31 | 15/10, p = 0,42 |
| DomainScoped | 47/4, p = 2,4·10⁻¹⁰ | 48/2, p = 2,3·10⁻¹² |

Taksonomia dla Graph@5:
- 7B: 424 błędne = TABLE_MISSING 325, COLUMN_HALLUCINATION 59, SEMANTIC_MISMATCH 38, JOIN 2.
- 3B: 415 błędnych = TABLE_MISSING 350, COLUMN_HALLUCINATION 45, SEMANTIC_MISMATCH 19, JOIN 1.
- TRUNCATION = 0 we wszystkich strategiach.
- Pełna tabela: `doc/chapters/tabele/tab_taksonomia.tex`.

### A. Ablacja M_exp (k = 5)

| M_exp | 0 | 1 | 2 | 3 | 5 |
|---|---|---|---|---|---|
| Recall@5 [%] | 40,45 | 52,92 | 55,12 | 55,28 | 55,28 |
| Tokeny DDL | 1161,5 | 1401,1 | 1479,1 | 1485,9 | 1485,9 |

- Reguła z konfiguracji (próg 0,5 pp) wskazuje **M_exp = 2**: przyrost do 3 wynosi 0,157 pp. Kod używa 3.
- Czasy przy M_exp = 3 (n = 500, mediana/p95 [ms]):

| Etap | Mediana | p95 |
|---|---|---|
| RRF | 18,03 | 21,99 |
| Wczytanie grafu | 0,158 | 0,173 |
| Mostki | 0,182 | 0,291 |
| Synteza DDL | 0,844 | 0,918 |
| Całość | 19,24 | 23,17 |

### A. TTFT
- 30 powtórzeń na punkt, nonce przeciw cache prefiksu, brak obcięć.

| Profil | Tokeny promptu | TTFT, mediana [ms] |
|---|---|---|
| 7B | 1204 | 587,5 [581,5; 593,9] |
| 7B | 28 123 | 7289 |
| 3B | 1203 | 484 |
| 3B | 28 122 | 4714 |

- Nachylenie: 7B 245,6 ms/1k tokenów [242,9; 248,2]; 3B 156,5 [153,8; 159,0].
- Dla 7B zależność jest niemonotoniczna między ~2269 a ~4153 tokenami (1092,7 → 1079,8 ms).

### B1. OOB
Tryby:
- loopback: `measured_loopback`;
- LAN: `emulated` (RTT 1 ms, 1000 Mb/s, jitter 0);
- WAN: `emulated` (RTT 30 ms, 250 Mb/s, jitter ±2 ms).

Tryb emulowany: zmierzone lokalnie składowe CPU + obliczone opóźnienie sieci.
- In-Band: 1×RTT + |ramka|/B.
- HTTP: 2×RTT + (|deskryptor| + |ładunek|)/B.

| S* [kB = 1024 B] | wartość | 95% CI |
|---|---|---|
| loopback SHM | 12,90 | [12,21; 13,54] |
| loopback HTTP | 25,26 | [24,47; 26,11] |
| LAN HTTP (model) | 122,85 | [121,73; 123,78] |
| WAN HTTP (model) | 1570,3 | [1554,9; 1585,7] |

- Przyspieszenie przy 50 MB na loopback (In-Band/SHM): 6,65×.
- Narzut ramki In-Band: 33,33% przy dużych ładunkach, 75,98% przy 512 B.
- Składowe przy 50 MB [ms]: enc 101,1, dec 165,5, SHA 31,9 + 31,9, kopiowanie do SHM 27,9.
- Globalne dopasowania liniowe (`oob_model_fit.json`) mają ujemne wyrazy wolne, więc ich nie interpretowano. Próg wyznaczono lokalnie, z dwóch sąsiednich rozmiarów.

H2, Mann–Whitney, n = 50/50, loopback:

| Rozmiar | SHM, mediana [ms] | In-Band, mediana [ms] | U | p | r | Szybszy |
|---|---|---|---|---|---|---|
| 2 KB | 0,1036 | 0,0304 | 2500 | 7,05·10⁻¹⁸ | −1 | In-Band |
| 10 MB | 14,73 | 112,96 | 0 | 7,07·10⁻¹⁸ | 1 | SHM |

Dla 10 MB różnica median wynosi −98,23 ms [−98,46; −98,03].

### B2. Paginacja (10 powtórzeń na punkt)

Keyset:
- 0,2575 ms przy offsecie 0, ok. 0,43 ms przy dalszych offsetach.
- Nachylenie 0,0032 ms/100 tys. wierszy, p = 0,58 (nieistotne).

OFFSET:
- Nachylenie 38,25 ms/100 tys. wierszy, R² = 0,9986.
- Przy 950 tys.: mediana 357 ms (341–617), czyli 829× keyset.

Unindexed (≤ 50 000): 187,3–220,7 ms, mediana 211,6.

Predykat DNF vs UNION ALL przy 950 tys.:
- mediany: 400,7 vs 0,42 ms;
- EXPLAIN DNF: Filter, Rows Removed = 950 000;
- EXPLAIN UNION ALL: 5 węzłów Index Cond, Merge Append, ok. 20 stron bufora.

Spójność: 10 000 rekordów, dokładna zgodność, 0 duplikatów, 0 brakujących, 51 stron. Brak odczytów fantomowych.

### C1. Współbieżność (500 zadań asyncio w jednym procesie; walidator wywoływany bezpośrednio)

| Wariant | RPS (M = 1 / 100 / 500) | Lag, mediana [ms] (M = 1 / 100 / 500) | Lag p99, M = 100 [ms] | PSS, M = 100 [GiB] |
|---|---|---|---|---|
| SYNC | 286 / 295 / 301 | 8,67 / 974,7 (n = 33) / 4919 (n = 6) | — | 1,04 |
| THREAD(4) | 259 / 257 / 263 | 1,47 / 76,6 / 67,7 | 383 (M = 500: 2031) | 1,05 |
| POOL(4) | 220 / 1065 / 1166 | 0,51 / 0,40 / 0,39 | 117–148 | 3,78 |

- POOL przy M = 100: 2 pracowników → 596 RPS; 8 pracowników → 1727 RPS (6,6 GB PSS).
- THREAD przy M = 100: 2 wątki → 270 RPS; 8 wątków → 259 RPS.

H3, Mann–Whitney POOL vs SYNC:
- M = 100: n = 2227/33, p = 5,36·10⁻²³, różnica median −974,3 [−994,7; −703,5] ms.
- M = 500: n = 2200/6, p = 2,28·10⁻⁵.

### D. Bezpieczeństwo (77 unikalnych wektorów)

| Miara | k/n | % | 95% CI CP |
|---|---|---|---|
| FPR (ANALYTICAL_SELECT) | 0/25 | 0,0 | [0; 13,7] |
| AMR (SQL_INJECTION) | 24/24 | 100,0 | [85,75; 100] |
| SIDE_EFFECT_SELECT przepuszczone | 15/15 | 100,0 | [78,2; 100] |
| Odsetek rekordów z pozostałymi tokenami sterującymi | 0/13 | 0,0 | [0; 24,7] |

- Średni czas walidacji [ms]: ANALYTICAL 2,61, SQLi 0,33, SIDE_EFFECT 0,96, PI 0,067.
- p95 dla wszystkich kategorii: 3,43 ms.
- Narzut XML: 31–50 tokenów (mediana 38 dla rekordów PI).
- llm_injection (3B, reguła regex): raw 7/13, xml 8/13; McNemar 1 vs 2 pary niezgodne, p = 1,0.
- Reguła regex dopasowuje także cytowanie treści rekordu (np. „administrator”), więc jest górnym oszacowaniem.

### Dryf schematu
- 50 tabel. Przeładowanie 364,5 ms: osadzenia 49,6, zapis 146,4, BM25 123,5, graf 5,69 ms.
- Mediana zapytań: 8,72 → 25,87 ms (p95 68,2) w trakcie przeładowania → 11,04 ms po nim.
- 0 błędów. 100% nowych tabel wykrytych.

## 3. Werdykty hipotez

Brzmienie hipotez ustalono **przed** odczytem werdyktów, według projektu testów zapisanego w konfiguracji rundy. Log zmian brzmienia: `RAPORT_AKTUALIZACJI_PRACY.md`, §2.

| Hipoteza / PB | Wynik | Werdykt |
|---|---|---|
| H1 Table-Recall, Graph@5 vs RRF@8 i BM25@8 | RRF@8: p = 2,1·10⁻¹⁴, r = 0,56. BM25@8: p = 0,032, r = 0,15. Oba istotne po korekcie Holma (2 testy). | **Potwierdzona** (efekt vs BM25@8 mały) |
| H1b Column-Recall, ta sama para | RRF@8: p = 0,009, r = 0,18 (Graph lepszy). BM25@8: p = 9,2·10⁻⁶, r = −0,30 (Graph gorszy). | **Częściowo potwierdzona** |
| H2 SHM vs In-Band (loopback) | 2 KB: In-Band szybszy. 10 MB: SHM szybszy. Oba p ≈ 7·10⁻¹⁸. | **Potwierdzona** dla jednego węzła. Progi LAN/WAN tylko z modelu. |
| H3 lag POOL vs SYNC | M = 100 i 500 istotne | **Potwierdzona**, z zastrzeżeniem: p99 POOL ok. 117–148 ms, pamięć ×3,6 |
| PB1 | Wyższa czułość przy równym budżecie. EX bez poprawy; 7B: BM25@8 istotnie lepszy. | Częściowo twierdząca |
| PB2 | S* na jednym węźle zmierzony. Sieć tylko z modelu. | Twierdząca (sieć: modelowo) |
| PB3 | POOL chroni medianę lagu. AMR 24/24, FPR 0/25. 15/15 side-effect SELECT przepuszczone. Enkapsulacja XML nie zmniejszyła sukcesu llm_injection. | Częściowo twierdząca |
| Hipoteza nadrzędna | — | **Częściowo potwierdzona** |
| C2 (SSE) | brak danych | **Niezweryfikowane**. Żadna hipoteza nie zależała wyłącznie od C2. |

## 4. Twierdzenia w pracy przed nową rundą → stan po nowej rundzie

| Twierdzenie (stara wersja tekstu) | Stan po rundzie |
|---|---|
| Pełny zrzut DDL ≈ 100,6 tys. tokenów (100 555) | 263 113 tokenów (jednolity format DDL) |
| Prompt o 97,8% mniejszy od pełnego zrzutu | Blok DDL o 99,4% mniejszy (porównanie DDL z DDL) |
| Table-Recall@5: 54,5% vs 39,2% (RRF) | 55,3% vs 40,4% (RRF@5); 47,0% (RRF@8); 53,2% (BM25@8) |
| Główny test H1: Graph@5 vs RRF@10 | Główny test: Graph@5 vs RRF@8 i BM25@8 (równy budżet z konfiguracji) |
| H1b: brak podstaw (vs RRF@10) | Częściowo: vs RRF@8 istotnie lepszy, vs BM25@8 istotnie gorszy |
| EX 15,8% vs 14,6%, różnica nieistotna | EX 15,2% (7B) / 17,0% (3B). Brak istotnej poprawy; 7B: BM25@8 istotnie lepszy (p = 0,029). |
| Profil A (CPU, max_tokens 120) / Profil B (GPU) | Profile 3B / 7B, oba GPU, num_predict 1024, num_ctx 8192 |
| TRUNCATION 30% niepowodzeń (Profil A) | TRUNCATION = 0 |
| Dominująca halucynacja kolumn (82,9%) | Dominuje TABLE_MISSING (325/424 dla 7B, 350/415 dla 3B) |
| Czas selekcji 20,6 ms (średnia) | Mediana 19,24 ms (p95 23,17) |
| M_exp = 2 i 3: ta sama czułość, przyjęto 3 | Reguła wskazuje 2 (przyrost do 3: 0,157 pp). Kod używa 3. |
| Brak pomiaru TTFT (wycofany) | Zmierzony: 157 / 246 ms na 1k tokenów (3B / 7B) |
| S*_emp = 5,19 kB w kontenerze | 12,9 kB na jednym węźle, bez kontenera |
| LAN 122,92 kB, WAN 1,68 MB | LAN 122,85 kB, WAN 1570 kB — **tylko z modelu** (emulated) |
| Przyspieszenie 6,87× przy 50 MB | 6,65× |
| Keyset 0,43 ms vs OFFSET 429 ms (991×) | 0,43 ms vs 357 ms (829×) |
| DNF vs UNION ALL mierzone lokalnie (inna maszyna) | Ta sama runda: 400,7 vs 0,42 ms |
| POOL 1173 RPS, lag 0,53 ms, +8,9 MB RSS | POOL(4) 1166 RPS przy M = 500, lag mediana 0,39 ms, p99 ~150 ms, PSS 3,8 vs 1,04 GiB |
| SYNC lag 3357/8702 ms | 975 ms (M = 100), 4919 ms (M = 500) |
| SSE 1,13 ms vs polling 66/127 ms | **Usunięte** (C2 nieukończony) |
| N = 1000 wektorów; AMR 100% [98,5; 100]; ASR 0% | 77 unikalnych wektorów; AMR 24/24 [85,8; 100]; FPR 0/25 [0; 13,7]; odsetek pozostałych tokenów sterujących 0/13 |
| — | **Nowe:** 15/15 side-effect SELECT przepuszczone; enkapsulacja nie zmniejsza llm_injection (7/13 vs 8/13) |
| Walidacja 1,30 ms średnio | ANALYTICAL 2,61 ms, SQLi 0,33 ms, p95 3,43 ms |
| Dryf 344,6 ms (pomiar pomocniczy) | 364,5 ms |
