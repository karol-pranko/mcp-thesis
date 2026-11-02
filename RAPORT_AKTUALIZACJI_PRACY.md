# Raport aktualizacji pracy po rundzie `20261004T120209_d347d2`

Gałąź: `aktualizacja-wynikow`, utworzona z `main` (774b75a). Ta wersja zawiera korekty Etapu A (6cf0ac6), audyt (b2744c8) i naprawę aparatury (4277ab8). **Nic nie zostało zacommitowane.** Szczegóły liczbowe i inwentaryzacja: `ANALIZA_WYNIKOW.md`.

## 1. Stan rundy

- Runda `full`, commit 774b75a z flagą `dirty`.
  - Jedyna zmiana: niezacommitowana poprawka `benchmarks/common/db.py` (`reload_schema_state()` po `DISCARD ALL`; usuwa błąd `InvalidSQLStatementNameError`).
  - Poprawkę zastosowano w rundzie.
  - Pozostaje w drzewie roboczym jako niezacommitowana. Warto ją zacommitować osobno.
- Sumy SHA-256 zgodne: 84/84. Brak podstawienia modeli.
- Scenariusze:
  - ukończone: wszystkie z wyjątkiem **streaming (C2)**;
  - `validation` nie przeszła wyłącznie z powodu C2.
- Przyczyna awarii C2: klient odpytujący co 5 s dostał `RemoteProtocolError`. Najpewniej to wyścig `timeout_keep_alive` uvicorn (5 s) z interwałem 5 s. Częściowe dane z `_streaming_tmp/` nie zostały użyte.
- B1 LAN/WAN: tryb `emulated`, czyli ekstrapolacja modelem.
  - Dane wejściowe: zmierzone składowe CPU plus założone RTT i przepustowość.
  - LAN: 1 ms, 1000 Mb/s.
  - WAN: 30 ms ±2 ms, 250 Mb/s.
  - Tak to opisano w pracy (6.3.1, tab. S*, 7.2, 7.4).
- `llm_injection` uruchomiono flagą, choć konfiguracja ma `false`. Fakt odnotowano w manifeście; wynik opisano w 6.6.

Stan drzewa przed moją pracą (zachowany, nie nadpisany):
- niezacommitowane zmiany użytkownika w `doc/chapters/wstep.tex`:
  - usunięte „(ang. \emph{model isolation})”;
  - dodane twarde spacje `~`;
  - usunięte „(Event Loop Starvation)”;
- zmiany użytkownika w `doc/main.pdf`;
- moja poprawka `db.py` (patrz wyżej).

Moje edycje `wstep.tex` zostały naniesione na wersję z poprawkami użytkownika.

## 2. Werdykty i zmiany brzmienia hipotez

Brzmienie zmieniłem wyłącznie po to, by odpowiadało projektowi testów zapisanemu w `benchmark_config.json`. Decyzję podjąłem **przed** odczytem werdyktów.

| Hipoteza | Stare brzmienie | Nowe brzmienie | Uzasadnienie | Werdykt |
|---|---|---|---|---|
| H1 | Graph@5 vs RRF@10 (główny); BM25@10 pomocniczo | Graph@5 vs RRF@8 **i** BM25@8 (równy budżet = maks. liczba tabel Graph@5); Wilcoxon z korektą Holma | Konfiguracja definiuje `equal_budget: k = 8, strategies BM25, RRF` | **Potwierdzona** (vs BM25@8: p = 0,032, r = 0,15, efekt mały) |
| H1b | Column-Recall vs RRF | Column-Recall vs RRF@8 i BM25@8 | Jak wyżej | **Częściowo potwierdzona** (vs BM25@8 Graph istotnie gorszy) |
| H2 | „istnieje próg S*, powyżej którego OOB istotnie szybszy” (test: Wilcoxon 10 MB) | Dodano: „…a poniżej którego przewagę ma Base64”; test Manna–Whitneya dla rozmiaru poniżej (2 KB) i powyżej (10 MB) progu | `h2_sizes_bytes: [2048, 10485760]`; próby niezależne, więc nie test sparowany | **Potwierdzona** (jeden węzeł) |
| H3 | Eliminacja blokowania pętli **i** ograniczenie podatności na wstrzykiwanie (opisowo AMR/FPR/ASR) | Istotnie niższe opóźnienie pętli POOL vs SYNC przy M = 100 i 500 (Mann–Whitney); aspekt bezpieczeństwa przeniesiony do PB3 | `h3_levels: [100, 500]`; dla bezpieczeństwa projekt nie przewiduje testu wnioskującego | **Potwierdzona** (zastrzeżenie: p99 POOL ~150 ms, pamięć ×3,6) |
| PB1–PB3 | bez zmian | bez zmian | — | PB1 częściowo; PB2 twierdząco (sieć: modelowo); PB3 częściowo |
| Nadrzędna | bez zmian | bez zmian | — | **Częściowo potwierdzona** (wcześniej: „potwierdza”) |
| C2/SSE | brak osobnej hipotezy | — | — | niezweryfikowane; usunięte z wyników |

Inne zmiany w opisie metod, wynikające z kodu aparatury:
- EX porównuje multizbiory wartości wierszy (porządek tylko przy `ORDER BY` w GT), bez nazw kolumn;
- wskaźnik ASR zastąpiono „odsetkiem rekordów z pozostałymi tokenami sterującymi” (`sanitization_residual_rate`);
- podatność samego modelu opisano osobno (llm_injection).

## 3. Zmiany w plikach .tex

| Plik | Zmiany |
|---|---|
| `doc/EE_dyplom__Copy_/EE-dyplom.tex` (cel `doc/main.tex`) | `\input{chapters/wyniki_liczby}` w preambule. Nowe zdania wynikowe streszczenia PL i abstraktu EN (makra; wersje `…En` w formacie angielskim). Symlink zachowany (edycja przez realpath). |
| `doc/chapters/wyniki_liczby.tex` (nowy, generowany) | 231 makr `\wyn…` z komentarzem źródła. Polecenie `\tabelawynikow`. |
| `doc/chapters/tabele/*.tex` (nowe, generowane) | 12 ciał tabel: selekcja, text2sql, mcnemar, taksonomia, ttft, sstar, paginacja, predykat, explain, wspolbieznosc, bezpieczenstwo, hipotezy |
| `wstep.tex` | 1.2: pełny zrzut 263 113 tokenów + nachylenie TTFT. 1.5: H1/H1b/H2/H3 (patrz §2). 1.6: opis rundy i testów. 1.7: opis rozdz. 5 i 6. |
| `protokoly.tex` | 3.1.1 „Most logiczny”: S* = 12,9 kB (zamiast 5,19 kB i niezweryfikowanego 4,8–5,2 kB). 3.3.2: „poniżej 1 ms” → zmierzone 2,61 ms (p95 3,43 ms). |
| `projekt.tex` | 4.3: pełny zrzut 263 113. 4.3.4: czasy (mediana mostków 0,18 ms, całość 19,24 ms) zamiast 20,6 ms. 4.4: zdanie o „pełnej spójności z benchmarkami” zastąpione odesłaniem; LAN/WAN jako model. |
| `implementacja.tex` | 5.1: zdanie, że runda działała natywnie, bez Dockera. 5.5: próg 12,9 kB. 5.7: profile 3B/7B (GPU, 1024 tokenów, num_ctx 8192) zamiast Profilu A/B; protokół harnessu (DISCARD ALL per zapytanie, ziarna, rozgrzewki, PSS, manifest). Tabela 6: wiersze TTFT i paginacji, liczności, testy. |
| `wyniki.tex` | Przepisany w całości (szczegóły w §5). Etykiety cytowane z innych rozdziałów zachowane. |
| `podsumowanie.tex` | 7.1 pkt 5–6; tabela 13 (nowa treść i werdykty); akapity 7.2; tabela 14 (bez wiersza SSE, nowe koszty); 7.4: nowe punkty (dryf z rozdz. 6, ograniczony wpływ selekcji na EX, warunki pomiaru z **zdaniem o nieukończonym C2**), side-effect SELECT, enkapsulacja; 7.5 pkt 2 (TABLE_MISSING zamiast halucynacji kolumn). |

Rysunki (`doc/rysunki`) wygenerowano z `results-remote-new-sunday` za pomocą `generate_thesis_plots.py --raw-dir … --output-dir doc/rysunki`:
- schema_recall_precision, text2sql_accuracy, error_decomposition_tree, ablation_mexp, ttft_scaling (nowy), oob_crossover_rtt, pagination_depth_1m, concurrency_rps_lag;
- brak rysunku streamingu.

Zmiany w `generate_thesis_plots.py` (logika danych bez zmian, z jednym wyjątkiem w pkt 4):
1. Przesunięcie adnotacji i legend (recall, ablacja, współbieżność).
2. Stałe kolory wariantów.
3. Etykieta osi „zadania asyncio – klienci – w jednym procesie”.
4. **TTFT:** grupowanie po `target_ddl_tokens` zamiast po dokładnym `prompt_eval_count`. Nonce zmieniał liczbę tokenów, co dzieliło punkt 30 powtórzeń na grupy po 1–9; legenda była niezgodna z projektem.

Test `tests/test_no_synthetic_data.py` przechodzi.

Kompilacja (`make xelatex` w kopii roboczej, timeout 300 s):
- 0 błędów, 0 niezdefiniowanych odwołań;
- overfull boxów 10 (przed zmianami 11).

`doc/main.pdf` zastąpiony nowym PDF.

## 4. Tabela pochodzenia (kluczowe wartości)

Pełna lista: komentarze nad każdym makrem w `doc/chapters/wyniki_liczby.tex` (plik źródłowy + sposób obliczenia).
- Skrypty: `benchmarks/analysis_final/analiza.py` → `out/wyniki.json` → `generuj_makra.py` → makra i tabele.
- Kontrola: `sprawdz_makra.py`.

| Wartość | Makro | Miejsce | Plik źródłowy |
|---|---|---|---|
| 263 113 tokenów | `\wynFullDumpTokens` | streszczenie, 1.2, 4.3, 6.2, tab. 13 | `schema_full_dump_tokens.json` |
| 99,4% | `\wynReductionVsFull` | streszczenie, 6.2, tab. 13–14 | jw. + `schema_retrieval.parquet` |
| 1482 tokenów DDL Graph@5 | `\wynDdlGraphFive` | streszczenie, 6.2, tab. 14 | `schema_retrieval.parquet` |
| 55,3 / 47,0 / 53,2% | `\wynRecallGraphFive`, `\wynRecallRrfEight`, `\wynRecallBmEight` | streszczenie, 6.2, 6.7, tab. 13 | `schema_retrieval.parquet` |
| p = 2,1·10⁻¹⁴; 0,032 | `\wynHjedenRrfP`, `\wynHjedenBmP` | 6.7, tab. 13 | `statistical_hypothesis_tests.csv` |
| EX 15,2 / 17,0 / 17,8% | `\wynExLargeGraphFive`, `\wynExSmallGraphFive`, `\wynExLargeBmEight` | streszczenie, 6.2, 7.2, 7.4 | `schema_quality_generation.parquet` |
| McNemar p = 0,029 | `\wynMcLargeBmEightP` | 6.2, 6.7, tab. 13 | `statistical_hypothesis_tests.csv` |
| 325/424 TABLE_MISSING | `\wynErrTableMissingLarge`, `\wynErrFalseLarge` | 6.2.4, 7.5 | `schema_quality_generation.parquet` |
| M_exp sweet spot = 2 | `\wynAbSweet` | 6.2.5 | `ablation_m_exp_sweet_spot.json` |
| 19,24 ms selekcja | `\wynTTotalMed` | 4.3.4, 6.2.5, 7.3 | `ablation_m_exp_calls.parquet` |
| 245,6 ms/1k tokenów | `\wynTtftSlopeLarge` | 1.2, 6.2.6 | `statistical_hypothesis_tests.csv` (TTFT) |
| S* 12,9 kB (loopback SHM) | `\wynSstarLoopShm` | streszczenie, 3.1.1, 5.5, 6.3, 7.x | `oob_crossover_metrics.json` |
| S* LAN 122,9 / WAN 1570,3 kB | `\wynSstarLan`, `\wynSstarWan` | 6.3, tab. 13–14 | jw. (emulated) |
| 6,65× | `\wynSpeedupFifty` | streszczenie, 6.3, tab. 13–14 | `oob_crossover_results.parquet` |
| H2 p | `\wynHdwaSmallP`, `\wynHdwaBigP` | 6.3, tab. 13 | `oob_h2_tests.json` |
| 0,43 / 357 ms; 829× | `\wynKeysetDeep`, `\wynOffsetDeep`, `\wynRatioDeep` | streszczenie, 6.4, tab. 13–14 | `pagination_advanced_1m.csv` |
| DNF 400,7 / UNION 0,42 ms | `\wynDnfDeep`, `\wynUnionDeep` | 6.4.2 | `pagination_predicate_ablation.csv` |
| Lag 975 → 0,40 ms (M = 100) | `\wynCcSyncHundredLagMed`, `\wynCcPoolHundredLagMed` | streszczenie, 6.4.3, tab. 13 | `concurrency_summary.csv` |
| PSS 1,04 / 3,78 GiB | `\wynCcSyncHundredMem`, `\wynCcPoolHundredMem` | streszczenie, 6.4.3, tab. 13–14 | jw. |
| H3 p | `\wynHtrzyHundredP`, `\wynHtrzyFivehundredP` | 6.4.3, tab. 13 | `concurrency_h3_tests.json` |
| AMR 24/24, FPR 0/25, side-effect 15/15, residual 0/13 | `\wynAmr…`, `\wynFpr…`, `\wynSide…`, `\wynResid…` | streszczenie, 6.6, tab. 13–14, 7.4 | `security_suite_metrics.json` |
| llm_injection 7/13 vs 8/13 | `\wynLlmRawK`, `\wynLlmXmlK`, `\wynLlmP` | 6.6, tab. 14 | `security_llm_injection.jsonl` |
| Dryf 364,5 ms | `\wynDriftTotal` | 6.4.4, 7.4 | `schema_drift_summary.json` |
| Sprzęt i oprogramowanie | `\wynGpu`, `\wynCpu`, `\wynPg`, … | 6.1.2 | `environment.json` |

## 5. Usunięta treść

- **Sekcja 6.5** (Scenariusz C2, SSE vs polling) w całości, wraz z rysunkiem `streaming_sse_vs_polling.pdf` (odwołanie) i miarą TTFE w 6.1.1.
- Wiersz „Asynchroniczne SSE” z tabeli 14.
- Liczby SSE (1,13 ms, 66–126 ms, 0,00 B/s).
- Opis projektu i implementacji SSE/Kafka w rozdz. 4–5 pozostawiono.
- Zdanie o nieukończonym pomiarze dodano w 7.4.
- Wszystkie liczby ze starych przebiegów (lista z Fazy 6). Grep znajduje wyłącznie nowe wartości zbieżne przypadkiem (np. EX 15,8% RRF@5/3B, keyset 0,43 ms) oraz „kody 429” HTTP w rozdz. 3.
- Akapit 6.6.2 o wycofanym `llm_inference_latencies.csv`. TTFT jest teraz zmierzony.
- Argument |z|/√n w 6.1.3.
- Opis „Profil A (CPU, max_tokens 120) / Profil B”, akapit o niepotwierdzonym modelu 7B i punkt 7.4 o TRUNCATION / num_ctx. W nowej rundzie model jest potwierdzony w manifeście, TRUNCATION = 0, a obcięć brak.
- Stara tabela bezpieczeństwa (N = 1000, ASR) i stara tabela Wilcoxona (RRF@10, H2 Wilcoxon).
- Uwagi `% TODO-ETAP-A` w wyniki.tex: rozwiązane danymi tej rundy (listy GT zapisane, rozkład czasów per zapytanie, TTFT).

## 6. Gdzie teza słabnie, a gdzie się wzmacnia

**Słabnie:**
- EX: wariant autorski nie jest lepszy od prostszych metod. Na profilu 7B BM25@8 jest istotnie lepszy.
- H1b: Column-Recall jest istotnie gorszy od BM25@8.
- POOL:
  - pamięć ×3,6 (wcześniej „+8,9 MB”);
  - ogon lagu p99 ok. 150 ms (wcześniej „submilisekundowy”);
  - przy jednym kliencie wolniejszy od SYNC.
- Bezpieczeństwo:
  - przepuszczono 15/15 side-effect SELECT;
  - enkapsulacja XML nie zmniejsza podatności modelu 3B;
  - mniejsza próba unikalnych wektorów, więc szersze CI.
- Progi LAN/WAN pochodzą z modelu, nie z pomiaru.
- Wniosek nadrzędny brzmi teraz „częściowo potwierdzona”.

**Wzmacnia się:**
- H1 jest istotna przy rzetelnie wyrównanym budżecie, także względem BM25.
- Redukcja kontekstu liczona w jednolitym formacie (99,4%).
- TTFT zmierzony, z nachyleniem i CI.
- H2 rozstrzygnięta w obie strony progu.
- H3 rozstrzygnięta testem wnioskującym (wcześniej opisowo).
- Paginacja i ablacja predykatu w tej samej rundzie z 10 powtórzeniami i planami EXPLAIN.
- Pełna proweniencja: manifest, SHA-256, skróty modeli.

## 7. Sprawy otwarte i decyzje dla autora / promotora

- `% TODO-WYNIKI` w `wyniki.tex` (6.1.2): powtórzyć C2 po zmianie `timeout_keep_alive` (lub przy interwale innym niż 5 s). Do tego czasu SSE jest bez wyników.
- **M_exp:** reguła z konfiguracji wskazuje 2, a kod i eksperymenty używają 3 (różnica 0,16 pp recall, 6,7 tokenu). Do decyzji: zostawić 3 (tekst opisuje to uczciwie) albo zmienić domyślną wartość w kodzie, co wymaga nowej rundy.
- Pomiar sieci rzeczywistej (netem z NET_ADMIN albo dwa węzły), jeśli progi LAN/WAN mają być twierdzeniem pomiarowym.
- llm_injection: kryterium regex jest przybliżone (dopasowuje cytaty). Warto ocenić odpowiedzi ręcznie lub sędzią i rozważyć większą próbę lub model 7B.
- Zbiór pytań wymienia nazwy tabel, co sprzyja BM25 i ekspansji grafowej. Opisano to w 7.4. Promotor może chcieć uwagi w 1.6.
- Poprawka `benchmarks/common/db.py`: zacommitować (runda była `dirty` tylko z jej powodu).
- `results-remote-new-sunday/`, `benchmarks/analysis_final/`, `doc/chapters/tabele/`, `wyniki_liczby.tex`, `fig_ttft_scaling.*` są nieśledzone. Trzeba je dodać do repozytorium przy commicie (katalog wyników ma 46 MB; ewentualnie LFS lub archiwum).

## 8. Błędy poza zakresem zadania (niezmienione)

- `projekt.tex` 4.4: „$R_{\text{tok}} \approx 0{,}85-1{,}0$ tokena na bajt” bez źródła i bez pomiaru w rundzie.
- `protokoly.tex` 3.3.2: „opóźnienie zimnego startu rzędu 150–300 ms” piaskownicy, bez cytowania.
- `wstep.tex` 1.2: „okna kontekstowe rzędu 10⁶ tokenów”, bez cytowania.
- `implementacja.tex` tab. 5 opisuje konfigurację Docker Compose (`shared_buffers=1GB`, `work_mem=32MB`, `max_connections=200`). Runda działała natywnie z innymi parametrami (2,5 GB, 64 MB, 100). Dodano zdanie wyjaśniające; samej tabeli nie zmieniałem.
- `implementacja.tex` 5.7: opis klienta ReAct (`agent_loop.py`, StdioMcpClient/SseMcpClient) nie był używany w rundzie (benchmark woła Ollamę bezpośrednio). Zostawiono jako opis implementacji.
- `doc/rysunki/` zawiera wiele nieużywanych plików ze starych wersji, m.in.:
  - `streaming_sse_vs_polling.pdf`, `fig_ttft_le2e_boxplots.*`;
  - `latency_ttft_e2e_boxplot.pdf`, `schema_*.pdf`, `security_*.pdf`, `prompt_injection_asr_context_isolation.pdf`.

  Nie są wstawiane do pracy. Zalecane usunięcie, by nie zostały użyte przez pomyłkę.
- Przeformułowania stylistyczne poza zakresem (np. „rygorystyczny” w rozdz. 2–5) pozostawiono.
