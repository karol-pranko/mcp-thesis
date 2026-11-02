# Etap A: raport z korekty (selekcja schematu i Text-to-SQL)

Gałąź: `etap-a-korekta`. Eksperymentów nie uruchamiano ponownie. Wszystkie liczby przeliczono z `benchmarks/raw_results/` skryptami w `benchmarks/analysis_etap_a/`:

| Skrypt | Wynik | Zakres |
|---|---|---|
| `krok0_rozpoznanie.py` | `out/krok0.txt` | Krok 0: warianty, tokeny, kopie k, ablacja, pochodzenie danych rys. 10, liczba wierszy GT, okno kontekstu, czasy LLM |
| `h1_testy.py` | `out/h1_testy.csv`, `out/graph_tfinal_po_k.csv` | A4: Wilcoxon (konstrukcyjny i przy porównywalnej liczbie tabel), McNemar dla EX |
| `taksonomia_bledow.py` | `out/taksonomia_bledow_k5.csv`, `out/bledy_kolumn_komunikaty.csv` | A7 |
| `zbior_ewaluacyjny.py` | `out/zbior_gt_rozklad.csv`, `out/zbior_przyklady.txt` | A5 |
| `rysunki_etap_a.py` | `doc/rysunki/fig_{schema_recall_precision,text2sql_accuracy,error_decomposition_tree,ablation_mexp}.{pdf,png}` (+ kopie w `doc/figures`, `figures`, `benchmarks/plots`) | rys. 2–5 (przez `plot_generators/generate_thesis_plots.py`) |

Uruchomienie: `cd benchmarks/analysis_etap_a && ../../.venv/bin/python <skrypt>.py`. Wspólne ładowanie danych jest w `common.py`. Źródłem per-zapytaniowym jest `schema_quality_results.parquet`, identyczny bajtowo z `schema_quality_n1000_dual_engine.*`.

Praca kompiluje się bez błędów (`make xelatex`, 100 stron) i bez niezdefiniowanych odwołań. Liczba ostrzeżeń overfull hbox jest taka sama jak przed zmianami (11).

---

## 1. Krok 0: odpowiedzi

**1. Ewaluowane warianty; tożsamość „Baseline A”.**
`raw_results` zawiera 5 strategii × 2 profile × k ∈ {3, 5, 10, 20} × 500 zapytań, łącznie 20 000 wierszy.
- „Baseline A (Full Schema)” to **15 tabel domeny zapytania**, a nie pełny zrzut. Kod: `bench_schema_quality.py` l. 724–730 (`SELECT table_name FROM schema_embeddings WHERE domain = $1 LIMIT 15`). Średni prompt ma 1032,2 tokenu (min. 945, maks. 1136).
- **Pełny zrzut 1000 tabel (100 555 tokenów) nie był ewaluowany pod kątem EX ani TTFT.** Wartość pochodzi wyłącznie z pomiaru objętości w `benchmark_schema.py` (`fetch_naive_ddl`), plik `schema_scaling_analysis.csv`.

**2. Dane do rys. 10 (TTFT, L_E2E).**
`llm_inference_latencies.csv` **nie jest pomiarem**. Wszystkie 400 wartości są identyczne (różnica 0,000) z `np.clip(np.random.normal(...))` przy `np.random.seed(2026)` z `benchmarks/plot_generators/generate_publication_plots.py` l. 1121–1131. Ten sam model z seed 42 jest w `scripts/generate_schema_discovery_and_pagination_plots.py` l. 250–267. Weryfikacja: `krok0_rozpoznanie.py`, pkt 4.
- Próba: N = 100 na grupę, symulowana.
- Brak informacji o tokenach promptu; komentarz w kodzie podaje „925 vs 100 555 tokenów”.
- Test Wilcoxona dla TTFT nigdzie nie był policzony; adnotacja „W = 0, p < 10⁻¹⁵” była wpisana na sztywno.

**3. Dane per zapytanie.**
Dla każdej strategii, profilu i k zapisano: `table_recall`, `table_precision`, `column_*`, `schema_complete`, `bridge_tables_found`, `valid_sql`, `ex_match`, `error_category`, `error_message`, `generated_sql`, `prompt_tokens` i `latency_ms`.

**Brakuje list zwróconych tabel, list tabel GT i treści pytań.** Mimo to:
- |GT| da się odtworzyć jako |T|·P/R; zgodność z szablonem wynosi 500/500.
- |T_final| wariantu autorskiego = k + `bridge_tables_found`; zgodność z P/R wynosi 1921/1921.

**Wywołanie LLM wykonano tylko dla k = 5.** Wiersze dla k = 3, 10, 20 kopiują SQL, Valid, EX, kategorię błędu i latencję z k = 5 (`bench_schema_quality.py` l. 1092–1202; weryfikacja: `krok0`, pkt 2). Metryki IR są natomiast prawdziwe dla każdego k i identyczne w obu profilach.

**4. `num_ctx`.**
Nie ustawiono go nigdzie: `agent_loop.py` l. 128–133 przekazuje tylko `temperature`, `num_predict` i `stop`, a grep po repozytorium nie znajduje `num_ctx`.

Prompty przy k = 5 (tokeny `cl100k_base`):

| Strategia | Średnio | Maksimum | Prompty > 2048 | Prompty > 4096 |
|---|---|---|---|---|
| Graph | 2218,8 | 3085 | 361 / 500 | 0 |
| pozostałe | ≤ 1032,2 | ≤ 1136 | 0 | 0 |

Obcięcie kontekstu jest możliwe, jeśli domyślne okno serwera było mniejsze. Uwaga dodatkowa: lista `stop` zawiera `";"` oraz `"\n\n"`.

**5. Definicja „Valid SQL” w kodzie.**
To `EXPLAIN` plus faktyczne wykonanie bez wyjątku (`_verify_sql_in_postgres`, l. 944–949), a nie parsowanie AST. EX porównuje listy słowników `[dict(r)]`, więc liczą się nazwy kolumn (aliasy) i kolejność wierszy (l. 962–964).

**6. Dane w 1000 tabelach.**
`generate_enterprise_schema.py` tworzy tabele bez wierszy. `seed_enterprise_tables.py` zasila 10–50 wierszy na tabelę, ale nie jest wywoływany przez żaden skrypt orkiestracji. Dane świadczą jednak, że tabele były zasilone:
- 0 odrzuceń „VACUOUS_TRUTH” na 20 000 wierszy, 539 trafień EX przy k = 5;
- w komunikatach SEMANTIC_MISMATCH zapytanie GT zwracało od 5 do 50 wierszy (mediana 33, n = 128) przy `LIMIT 50`.

Wolumen jest więc mały.

**7. Powstanie zbioru 500 pytań.**
Szablony w kodzie: `SyntheticQueryBenchmarkGenerator`, l. 410–636, `random.Random(2026)`. Dla każdej domeny generator losuje tabelę startową, wyznacza ścieżkę DFS w grafie FK i wypełnia nią jeden z szablonów:

| Typ | Na domenę | Liczba tabel |
|---|---|---|
| prosty | 30 | 2/3 |
| średni (GROUP BY/HAVING) | 50 | 3/4 |
| złożony (CTE) | 20 | 4/5/6 |

Przy N = 500 jest 100 zapytań na domenę. Pytanie **wymienia wprost wszystkie tabele GT i kolumny wynikowe**, ale nie podaje kolumn złączeń. Szablon średni każe użyć `HAVING total_count >= 1`; to alias, którego PostgreSQL nie akceptuje w `HAVING`.

Rozkład |GT|:

| Tabele | 2 | 3 | 4 | 5 | 6 |
|---|---|---|---|---|---|
| Zapytania | 75 | 200 | 160 | 35 | 30 |

- 85,0% zapytań ma ≥ 3 tabele, czyli zawiera tabelę pośrednią.
- Ekspansja dołączyła ≥ 1 tabelę GT w 243 / 500 zapytaniach.
- Odsetka GT z tabelą zaprojektowaną jako mostek N:M (180 takich tabel w `generate_enterprise_schema.py` l. 385) **nie da się ustalić**, bo listy GT nie zostały zapisane. Kolejność sąsiadów w DFS zależy od `PYTHONHASHSEED`, więc dokładne odtworzenie zbioru nie jest możliwe.

**8. Tokeny i rozbieżności między tab. 7 a rys. 5.**

Średnie tokeny promptu (tab. 7 liczy cały prompt: instrukcja + DDL + pytanie):

| k | A | BM25 | RRF | Dense | Graph |
|---|---|---|---|---|---|
| 3 | 1032,2 | 381,2 | 381,3 | 380,9 | 1486,1 |
| 5 | 1032,2 | 492,1 | 493,7 | 491,8 | 2218,8 |
| 10 | 1032,2 | 784,3 | 789,6 | 783,5 | 3977,5 |
| 20 | 1032,2 | 1435,4 | 1423,2 | 1393,9 | 7247,7 |

Ablacja (rys. 5) liczy `count_tokens(ddl)`, czyli **sam DDL** (l. 1364). Przyczyny rozbieżności:
- **M_exp = 0: 1622,5 vs RRF 493,7.** Zbiór tabel jest identyczny (recall 0,3924 w obu). Różnica wynika z formatu: Graph używa `_format_compact_enterprise_ddl` (nagłówek z pytaniem, opisy, wszystkie kolumny z typami, FK). Baseline'y używają `_format_ddl_for_tables` (maks. 10 kolumn, typ `TEXT`).
- **M_exp = 3: 1998,4 vs 2218,8.** Różnicę stanowią instrukcja i pytanie.
- Recall dla M_exp = 3 (0,5448) jest identyczny z Graph@5.

---

## 2. Lista zmian

### Tekst

| Miejsce | Było → jest (skrót) |
|---|---|
| Streszczenie PL i EN (`doc/EE_dyplom__Copy_/EE-dyplom.tex` l. 51, 57; `doc/main.tex` to symlink) | „redukcja konsumpcji tokenów DDL o 97,8% … skrócenie TTFT o 97,5% (1382,7 → 34,7 ms)” → „wycinek wstrzykiwany do promptu o ok. 97,8% mniejszy od pełnego zrzutu; Recall@5 54,5% wobec 39,2% (RRF) i 48,8% (RRF@10); EX 15,8% wobec 14,6% najlepszego baseline'u (nieistotne)”. Usunięto TTFT. |
| 1.5 (`wstep.tex` l. 94–95) | H1: test przy równym budżecie tabel (Graph@5 vs RRF@10), BM25 pomocniczo. H1b: dodano zdanie, że jest pochodną H1. |
| 4.3.4 (`projekt.tex` l. 391) | „poniżej 2 ms” → „średnio 20,6 ms na pełne wywołanie selekcji (ablacja)”. |
| 6.1.1 (`wyniki.tex` l. 45) | Valid SQL: „analiza AST” → „EXPLAIN i wykonanie bez błędu”. EX: dopisano wymóg nazw kolumn i kolejności wierszy. Dodano zdanie o małej liczbie wierszy (5–50, mediana 33) i ograniczonej wiarygodności EX. |
| 6.1.3 (`wyniki.tex` l. 109) | Usunięto argument „\|z\|/√n_eff = 0,87 – niezależna miara efektu”; wskazano, że przy W = 0 wynika on ze wzoru. Dodano opis testu McNemara. |
| 6.2.1 (`wyniki.tex` l. 120–134, nowe) | A5: sposób generacji, rozkład \|GT\|, 85,0% i 243/500, zdanie o możliwym uprzywilejowaniu metody, listing `lst:przyklady_gt` z dwoma instancjami szablonów, TODO. |
| 6.2.1 (l. 137) | Baseline A: „tokenów promptu; nie jest to pełny zrzut DDL 1000 tabel, który nie był ewaluowany”. |
| Tab. 7 | Nagłówek „Tokeny DDL” → „Tokeny promptu”. |
| 6.2.1 (l. 167) | Akapit 97,8%: zrobiono z tego porównanie objętości, nie jakości; pełnego zrzutu nie ewaluowano. Dodano zdanie o wyższym koszcie tokenowym wariantu autorskiego (2218,8 vs 492,1 / 493,7 / 491,8 / 1032,2) i o formacie DDL. |
| 6.2.1 (l. 171, 175) | Recall: dopisano, że wariant zwraca 5–8 tabel (śr. 6,04), a przewaga nad RRF przy tym samym k jest konstrukcyjna; wyniki przy k = 10. EX: dodano McNemar (różnica nieistotna) i uwagę o formacie DDL. |
| 6.2.2 (l. 189) | „dominuje … w całym spektrum k” → „najwyższa czułość przy każdym k, ale przy większej liczbie tabel (3,55 / 6,04 / 11,92 / 22,71)”. |
| 6.2.3 (l. 206–208, nowe) | A9: profile różnią się jednocześnie modelem i limitem; model 7B niepotwierdzony (TODO). Generacja tylko dla k = 5. |
| 6.2.3 (l. 214, 220, 222) | Podpis rys. 3 (definicja Valid SQL, k = 5, Baseline A = 15 tabel). „Full Schema” → „Baseline A”. Usunięto „przeładowanie promptu paraliżuje model”. |
| 6.2.4 (l. 233–244) | A7: liczności przeliczone z k = 5 per strategia; usunięto „200” i „508 przypadków” oraz wniosek o 2,2-krotnej redukcji TABLE_MISSING (dane mu przeczą). Silnik A: dominuje COLUMN_HALLUCINATION, a nie TRUNCATION. Zdanie o rzadkich błędach złączeń i dominacji halucynacji kolumn. Nowy podpis rys. 4. |
| 6.2.5 (l. 254–268) | Podpis rys. 5 bez „latencji grafowej”. Wyjaśnienie 1622,5 vs 493,7 i 1998,4 vs 2218,8 oraz uwaga o czynniku zakłócającym EX. Nowe uzasadnienie M_exp = 3. Latencja 20,6 ms (zakres 20,6–22,0), bez mediany (TODO). |
| 6.7.1 (l. 527–547) | Opis testu konstrukcyjnego vs testu głównego. Tab. 12 przebudowana (5 wierszy H1/H1b + H2). Nowy akapit wyników i akapit McNemara. |
| 6.7.2 (l. 553–554) | Usunięto rys. 10 i mediany TTFT/L_E2E (dane syntetyczne). Dodano opis rzeczywistych latencji wywołań LLM i TODO. |
| 6.8.1 (l. 569–570) | „Full Schema” → Baseline A i RRF. Usunięto argument o TABLE_MISSING 27,0 → 11,9%. Dodano brak istotnego wzrostu EX. |
| 7.2, tab. 13 (`podsumowanie.tex` l. 40–44, 52, 54) | Wiersze H1, H1b i PB1 z nowymi testami. PB1: „Odpowiedź częściowo twierdząca”. Akapit o werdyktach. Usunięto „krótsze TTFT i L_E2E”. |
| 7.3, tab. 14 i l. 89 | Wiersz JIT: 97,8% jako rozmiar promptu względem pełnego zrzutu; koszt ok. 4,5× dłuższego promptu niż RRF/BM25; 20,6 ms zamiast „≈ 2,1 ms / 4,9 ms”. |
| 7.4 p. 3 (l. 105) | Usunięto „wymaga co najmniej 1024 tokenów” → „TRUNCATION 30,0% niepowodzeń (134/446), na Silniku B 0”. Dodano zdanie o `num_ctx` (361/500 > 2048, 0 > 4096). |
| 7.5 (l. 130–132, nowe) | Punkt „Ograniczenie halucynacji kolumn”. |

### Rysunki i kod analityczny

| Plik / rysunek | Zmiana |
|---|---|
| Rys. 2 | Etykieta „Baseline A: Domain-Scoped (15 tabel)”. Adnotacja Graph@5 vs RRF@10 z p liczonym w generatorze zamiast wpisanego na sztywno p = 5,53·10⁻⁴². |
| Rys. 3 | Etykieta Baseline A. Oś: „zaakceptowanych przez EXPLAIN i wykonanych bez błędu”. |
| Rys. 4 | Nowy generator: 2 panele (silniki), 5 strategii, k = 5, odsetek EX = False w strategii. |
| Rys. 5 | Usunięto „Sweet Spot: M_exp = 2”. Nowa adnotacja „M_exp = 2 i 3: ta sama czułość; przyjęto 3 (+10,4 tokena)”. Oś DDL opisana jako „sam DDL”. |
| Rys. 10 | Usunięty z pracy. `fig_ttft_le2e_boxplots` wyłączono z `generate_all_plots`, z komentarzem o syntetycznym źródle. |
| `generate_thesis_plots.py` | Zmieniono tylko 5 powyższych funkcji (stała `BASELINE_A_LABEL`). Kodu bramy nie zmieniano. |

---

## 3. Liczby wprowadzone lub zmienione

Pliki źródłowe: SQ = `schema_quality_results.parquet`, AB = `ablation_m_exp_results.csv`, SC = `schema_scaling_analysis.csv`. Skrypty: K0 = `krok0_rozpoznanie.py`, H1 = `h1_testy.py`, TB = `taksonomia_bledow.py`, ZE = `zbior_ewaluacyjny.py`.

| Wartość | Miejsce | Plik | Sposób obliczenia (skrypt) |
|---|---|---|---|
| 100 555; 2218,8; 97,8% | streszczenie, 6.2.1, tab. 13–14 | SC; SQ | 1 − 2218,8/100 555 = 97,79% (K0, pkt 1) |
| 54,5 / 39,2 / 48,8 / 53,2 % Recall | streszczenie, 6.2.1, 6.7.1, 6.8.1, tab. 13 | SQ | średnia `table_recall`: Graph@5, RRF@5, RRF@10, BM25@10 (H1) |
| 15,8 / 14,6 % EX | streszczenie, 6.8.1, tab. 13 | SQ | średnia `ex_match`, profil B, k = 5 (H1) |
| 492,1; 493,7; 491,8; 1032,2; 789,6; 784,3 tokenów | 6.2.1, 6.2.5, 6.7.1, tab. 13 | SQ | średnia `prompt_tokens` (K0, pkt 1) |
| 4,5×; 2,8× | tab. 14, 7.3; 6.7.1 | SQ | 2218,8/493,7; 2218,8/789,6 |
| 5–8 tabel, śr. 6,04; 3,55 / 11,92 / 22,71 | 6.2.1, 6.2.2, 6.7.1 | SQ | `top_k + bridge_tables_found` (K0, pkt 6; H1) |
| 5,7 pp; 1,3 pp | 6.2.1 | SQ | różnice zaokrąglonych średnich 54,5−48,8; 54,5−53,2 |
| 75/200/160/35/30; 15,0/40,0/32,0/7,0/6,0 %; 425 (85,0%) | 6.2.1 | SQ | \|GT\| = \|T\|·P/R, zgodny z szablonem (ZE) |
| 243 (48,6%) | 6.2.1, 6.7.1 | SQ | liczba zapytań z recall Graph@5 > RRF@5 (ZE; H1) |
| 30/50/20 | 6.2.1 | SQ (`difficulty`) | 6000/10 000/4000 z 20 000 |
| 5–50, mediana 33 wiersze (n = 128) | 6.1.1 | SQ (`error_message`) | regex „Ground Truth returned N rows”, k = 5 (K0, pkt 5) |
| W = 8101,0; p = 1,78·10⁻⁸; r = 0,421; n_eff = 236; 158/78 | tab. 12, 6.7.1, tab. 13, rys. 2 | SQ | Wilcoxon Graph@5 vs RRF@10, `table_recall` (H1) |
| W = 15 587,5; p = 0,10; r = 0,115; n_eff = 265; 142/123 | tab. 12, 6.7.1, tab. 13 | SQ | Wilcoxon Graph@5 vs BM25@10 (H1) |
| W = 18 492,5; p = 0,79; r = −0,018; n_eff = 274; 62,2 vs 62,5 % | tab. 12, tab. 13 | SQ | Wilcoxon `column_recall` Graph@5 vs RRF@10 (H1) |
| 62,2 vs 67,2 %, p = 2,96·10⁻⁶ | 6.7.1 | SQ | Wilcoxon `column_recall` Graph@5 vs BM25@10 (H1) |
| W = 0; p = 5,53·10⁻⁴² / 9,91·10⁻⁴²; n_eff = 243 | tab. 12 (bez zmian, opisane jako konstrukcyjne) | SQ | H1 |
| McNemar 17/11 p = 0,34; 22/13 p = 0,18; 6/4 p = 0,75; 9/4 p = 0,27; 22/9 p = 0,029; 56/6 p = 2,97·10⁻¹¹ | 6.2.1, 6.7.1 | SQ | dokładny dwumianowy na parach niezgodnych, k = 5 (H1) |
| 446: 179 / 134 / 77 / 43 / 7 / 6 (40,1 / 30,0 / 17,3 / 9,6 / 1,6 / 1,3 %); 145 z 477 (30,4%) | 6.2.4, 6.8.1, 7.4 | SQ | `error_category`, k = 5, EX = False (TB) |
| 421: 349 / 50 / 20 / 2 (82,9 / 11,9 / 4,8 / 0,5 %); 0 SYNTAX; 0–5 SYNTAX w baseline'ach | 6.2.4, 7.5 | SQ | TB |
| 127 z 471 (27,0%); 19 z 427 (4,4%); 8 z 430 (1,9%) | 6.2.4 | SQ | TB |
| JOIN 0,0–0,9%; COLUMN 68,2–91,4%; 386 (RRF), 393 (BM25) | 6.2.4 | SQ | TB |
| 220 (`fk_*`), 61 (`total_count`) z 349 | 6.2.4 | SQ | klasyfikacja `error_message` (TB, `bledy_kolumn_komunikaty.csv`) |
| 1622,5; 1988,0; 1998,4; 10,4 tokena; 0,5448 | 6.2.5, rys. 5 | AB | wprost; 1998,4 − 1988,0 |
| 20,6 ms (20,6–22,0) | 4.3.4, 6.2.5, tab. 14, 7.3 | AB (`avg_graph_latency_ms`) | wprost (średnie; mediany brak) |
| 361/500 > 2048; 0 > 4096; 3085 maks. | 7.4 | SQ | `prompt_tokens` Graph, k = 5 (K0, pkt 5b) |
| mediany latencji LLM: 12,7 / 19,1 s (Graph); 12,5–12,6 / 18,1–18,5 s | 6.7.2 | SQ (`latency_ms`) | mediana, k = 5 (K0, pkt 5c) |

---

## 4. Nierozwiązane problemy (`% TODO-ETAP-A`)

1. **`wyniki.tex` 6.2.1.** Listy tabel GT i treści 500 pytań nie zapisano w `raw_results`. Nie da się więc podać odsetka GT z tabelą mostkującą N:M w sensie generatora schematu. Listing pokazuje instancje szablonów odtworzone generatorem; to ilustracja, a nie elementy ewaluowanego zbioru. W listingu usunięto polskie znaki, tak jak w istniejącym listingu w rozdz. 4.
2. **`wyniki.tex` 6.2.3.** Brak logu przebiegu źródłowego (pliki z 2026-10-03). Jedyny log, `benchmarks/logs/bench_schema_quality_20261002_225433.log`, pochodzi z wcześniejszego przebiegu o innych wynikach (np. recall 0,7427 przy M_exp = 3) i dla obu profili pokazuje `qwen2.5-coder:3b`. Zmiennej `LLM_MODEL_STRONG` nie ustawia żaden skrypt, a bez niej oba profile dostają ten sam model. **Użycie 7B w Profilu B jest niepotwierdzone.** Opis profili (3B/7B) zostawiono, zgodnie z poleceniem, by nie zmieniać nazw.
3. **`wyniki.tex` 6.2.5.** `ablation_m_exp_results.csv` zawiera tylko średnie latencji, więc nie ma mediany ani rozrzutu.
4. **`wyniki.tex` 6.7.2.** TTFT i L_E2E nie zostały zmierzone; dane rys. 10 są syntetyczne. Potrzebny osobny pomiar (z liczbą tokenów promptu i jawnym `num_ctx`). Polecenia A3 „wygeneruj ponownie rys. 10” nie wykonano, bo odtworzenie wykresu z danych `np.random` utrwalałoby fikcyjny wynik.

Inne problemy zauważone poza zakresem (nie zmieniano):
- `scripts/generate_streaming_and_security_plots.py` l. 86–104 także generuje próbki `np.random` dla wykresów SSE/bezpieczeństwa. Warto sprawdzić, czy którykolwiek wykres w pracy z nich korzysta.
- Twierdzenia nieweryfikowalne z danych: 4.3.4 („ponad 95% złączeń ERP/CRM opiera się na N:M”) i 7.1 p. 2 („eliminującym problem gubienia tabel mostkujących”).
- `schema_quality_react_token_cost.csv` podaje „4 tury ReAct”, ale wynikają one z 4 kopii k, a nie z tur. Praca tego pliku nie cytuje.
- Rozdz. 6 wydłużył się o ok. 3 strony i **kończy się teraz na stronie nieparzystej (73), więc s. 74 jest pusta** (por. commit E8). Usunięcie tej strony wymagałoby skrócenia tekstu poza zakresem Etapu A.

---

## 5. Ocena: które tezy wzmocnione, a które osłabione

**Wzmocnione lub utrzymane**
- **Redukcja objętości kontekstu względem pełnego zrzutu (97,8%).** Liczby są potwierdzone; teza jest teraz poprawnie zawężona do objętości promptu.
- **H1 względem RRF.** Przewaga utrzymuje się także przy porównywalnej liczbie tabel (Graph@5 vs RRF@10: +5,7 pp, p = 1,78·10⁻⁸, r = 0,42). To jest teraz test nieprzesądzony konstrukcyjnie, więc mocniejszy metodologicznie niż poprzedni W = 0.
- **Ablacja M_exp.** Jest spójna z wynikami głównymi (M_exp = 0 ≡ RRF@5, M_exp = 3 ≡ Graph@5). Rozbieżności tokenów wyjaśnia format DDL i to, czy liczono sam DDL, czy cały prompt.
- **Profil A vs B.** Kierunek wniosku (ucinanie przy 120 tokenach, brak TRUNCATION przy 1024) potwierdza się.

**Osłabione**
- **Przewaga nad BM25.** Przy porównywalnej liczbie tabel jest nieistotna (p = 0,10). Wynik to kompromis czułość/koszt (ok. 2,8–4,5× więcej tokenów), a nie dominacja.
- **H1b.** Jest pochodną H1 i przy równym budżecie tabel nie znalazła potwierdzenia; względem BM25@10 Column-Recall jest istotnie niższy.
- **EX.** Przewaga nad RRF/BM25 (15,8 vs 14,6 / 14,0%) jest nieistotna (McNemar). Dodatkowo zakłóca ją różnica formatu DDL i mała liczba wierszy w tabelach.
- **TABLE_MISSING.** Teza o redukcji błędów braku tabeli dzięki ekspansji upada: RRF i BM25 mają ich mniej niż Graph (19 i 8 vs 50).
- **TTFT / L_E2E (97,5%, 1382,7 → 34,7 ms).** Usunięte, bo dane były syntetyczne. Rzeczywiste czasy wywołań nie pokazują przyspieszenia wariantu autorskiego względem baseline'ów selekcyjnych.
- **Zbiór ewaluacyjny.** Pytania nazywają tabele wprost, a 85% ścieżek zawiera tabelę pośrednią. Konstrukcja zbioru może więc sprzyjać metodzie autorskiej.
