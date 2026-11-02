# Poprawki po recenzji – raport końcowy

**Gałęzie i wersje**

| Element | Wartość |
|---|---|
| Gałąź z poprawkami tekstu i analizami | `poprawki-recenzja` (od `main` = ea36e90) |
| Gałąź z poprawkami kodu | `post-review-fixes` (od `poprawki-recenzja`, commit 88ed272) |
| Kod z rundy pomiarowej | tag lokalny `runda-20261004-kod-774b75a` |

**Dane rundy**
- Runda `20261004T120209_d347d2` jest w `results-remote-new-sunday/`; katalog `benchmarks/raw_results/` w repozytorium zawiera stare dane.
- Analizy wtórne są zapisane w `results-remote-new-sunday/derived/`, razem z `MANIFEST.json` (skrypt, SHA-256 skryptu, commit, wejścia i wyjścia z SHA-256).
- Skrypty analiz są w `benchmarks/analysis_offline/`.
- Liczby z tych analiz trafiają do pracy wyłącznie przez makra w `doc/chapters/wyniki_wtorne.tex`; w tekście są oznaczone jako „analiza wtórna” z nazwą skryptu.

**Nowe pomiary sprzętowe:** nie wykonano żadnych. Nowe liczby w pracy pochodzą tylko z dwóch źródeł:
- przeliczeń plików rundy;
- obliczeń niezależnych od sprzętu: tokenizacja Base64, wzór KV-cache, graf schematu odtworzony deterministycznym generatorem.

**Kompilacja:** `cd doc && make` (latexmk + biber).
- 0 niezdefiniowanych cytowań i odwołań, 0 „??”.
- 12 ostrzeżeń overfull (przed zmianami 10–11).
- Dwa „ignored error: Infinite glue shrinkage” w `symbols.tex`; występowały już wcześniej i nie wpływają na wynik.
- Zero `\TODO` w tekście.
- Kontrola makr (`benchmarks/analysis_final/sprawdz_makra.py`): 311 zdefiniowanych = 311 użytych.

## 1. Zadania

| ID | Status | Główne pliki | Commit |
|---|---|---|---|
| K1 bibliografia | zrobione | `doc/latexmkrc`, `doc/Makefile`, `main.tex` (`% !BIB program = biber`) | 970f27d |
| F1 wzory | zrobione | `teoria.tex` (KV/GQA), `projekt.tex` (5–6, 18–23, Welford), `tests/test_keyset_nulls.py` | 485bb07 |
| F2 terminologia i fakty | zrobione | `teoria.tex`, `protokoly.tex`, `projekt.tex`, `implementacja.tex`, `podsumowanie.tex`, `wstep.tex`, `.bib` | e9ad258 |
| E edycja, rysunki, załączniki | zrobione | `symbols.tex`, `appendices.tex`, `EE-dyplom.cls`, `generate_thesis_plots.py`, `doc/rysunki/*` | a315456 |
| O1–O9 analizy wtórne | zrobione (O5 w f608fec) | `benchmarks/analysis_offline/*`, `derived/*` | cb667f9, f608fec |
| K2 zbiór A (tabele nienazwane) | częściowo: zawężenie tez zrobione; eksperyment niewykonalny bez nowej rundy | `wyniki.tex` 6.2, `podsumowanie.tex` 7.4–7.5, warianty w §6 | fa3b7e1, 5ac69bb |
| K3 język osadzeń | zrobione | 6.2.1, 6.2.2, 7.4 | fa3b7e1 |
| K4 SSE/Kafka | zrobione (tekst); poprawka keep-alive na `post-review-fixes` | abstrakty, 1.4, 5.x, 7.1, 7.4 | 5ac69bb, 88ed272 |
| K5 zakres pomiarów, tytuł | tekst zrobiony; tytuł i hipoteza do decyzji (§6) | 6.1.2, 1.6, 7.2, 7.4 | fa3b7e1, 5ac69bb |
| K6 migawki | zrobione | 4.2.3 (kompromis), 2.4.2/3.2.2 (złagodzone), 6.4.2, 7.4 | e9ad258, fa3b7e1 |
| K7 rola bazodanowa, llm_injection | zrobione | 6.1.2, 6.5, 7.4, `testbed/init-scripts/rola_mcp_reader.sql` | fa3b7e1 |
| M mechanizmy bez ewaluacji | zrobione | `tests/test_mechanizmy_bez_ewaluacji.py`, 4.2.2, 5.x, 7.1, tabela 22 | 0ac41b1 |
| C1 pamięć workerów, ogon p99 | zrobione (analiza + tekst); poprawka importów na `post-review-fixes` | 6.4.3, 7.4, `o8b_ogon_lag.py` | fa3b7e1, 88ed272 |
| B1 transfer OOB | zrobione | 4.4, 6.3, rys. 7, tabela S* | e9ad258, fa3b7e1 |
| Etap 4 spójność | zrobione | streszczenia PL/EN, 1.x, 7.x, tabele 21–22, `doc/main.pdf` | 5ac69bb |

Numeracja tabel zmieniła się po dodaniu nowych tabel:
- recenzenckie tabele 18–20 to teraz **tabele 19, 21 i 22**;
- tabela testów hipotez to 19, tabela hipotez i pytań badawczych to 21, macierz kompromisów to 22.

## 2. Inwentarz danych rundy

Pełny inwentarz: `derived/inwentarz.json`, skrypt `inwentarz.py`.

| Analiza | Potrzebne dane | Dostępne? |
|---|---|---|
| O1 | rekordy per zapytanie z czułością i EX (`schema_retrieval.parquet`, `schema_quality_generation.parquet`) | tak |
| O2 | wyniki McNemara (`statistical_hypothesis_tests.csv`) | tak |
| O3 | czułość i tokeny DDL dla k ∈ {3, 5, 8, 10, 20} | tak |
| O4 | treść pytań + katalog tabel; rankingi BM25@5 per zapytanie; graf FK | tak; katalog i graf FK z deterministycznego generatora `generate_enterprise_schema.py` (zgodność z logiem rundy: 1000 węzłów, 1174 krawędzie) |
| O5 | brak danych rundy (obliczenie niezależne); `tiktoken` cl100k_base dostępny lokalnie | tak |
| O6 | generator schematu | tak |
| O7 | składowe czasów OOB (enc, dec, sha, copy, read, alloc) | tak |
| O8 | surowe próbki sondy lag z `ts_ns` | tak |
| O9 | surowe odpowiedzi modelu, wyekstrahowany SQL, błędy PostgreSQL | tak; promptów nie zapisano, ale są odtwarzalne z pytania i DDL |
| K7 | odpowiedzi modelu w `security_llm_injection.jsonl` | tak |
| K6 | wynik testu izolacji | tylko wartość logiczna, bez liczby stron i zapisów (szczegóły w §4) |
| M | dane dla Welforda | tylko zdegenerowany test (wiersze ok. 19 tys. tokenów przy budżecie 3000, 40/40 przekroczeń) |
| M | dane dla IndexCatalogGuard i kursora HMAC | brak |

## 3. Wyniki analiz wtórnych

Wszystkie pliki są w `results-remote-new-sunday/derived/`.

**O1 – czułość pełna i EX (`o1_czulosc_a_ex.json`)**

| Strategia | Pełna czułość (Table-Recall = 1) |
|---|---|
| Ekspansja (Graph@5) | 20,2% |
| BM25@8 | 11,2% |
| RRF@8 | 8,0% |
| RRF@5 | 5,2% |

- EX przy pełnej czułości dla Graph@5: 50,5% [40,4; 60,6] (3B) i 43,6% [33,7; 53,8] (7B).
- Dla porównania BM25@8 przy pełnej czułości: 67,9% (3B) i 58,9% (7B).
- EX Graph@5 według liczby tabel ścieżki:
  - 2 tabele: 90,7% (3B) i 82,7% (7B);
  - 3 tabele: 8,5% i 7,0%;
  - 4–6 tabel: 0.
- Wniosek: dodatkowe zapytania z kompletnym wycinkiem to głównie ścieżki 3+ tabel, które w tym zbiorze prawie nigdy nie dają EX.

**O2 – korekta Holma (`o2_holm_mcnemar.json`)**
- Rodzina: 6 testów na profil.
- BM25@8 > Graph na 7B: p = 0,029 → p_Holm = 0,147 (nieistotne).
- Istotna pozostaje tylko przewaga nad Domain-Scoped.

**O3 – Pareto (`o3_pareto.json`, rys. `fig_pareto_recall_tokens`)**
- Front Pareto: BM25@3, Graph@3, BM25@5, Graph@5, Graph@10, Graph@20.
- Przy objętości DDL Graph@5 (1482 tokeny) interpolacja liniowa daje: BM25 50,4%, RRF 43,4%, Graph 55,3%.

**O4 – kontrola zbioru (`o4_identifier_match.json`)**
- **Identifier-Match: Table-Recall = Table-Precision = 100% na wszystkich 500 pytaniach (średnio 3,49 tabeli).**
- Ekspansja nałożona na BM25@5: recall 63,0%, precision 36,0%, pełna czułość 25,2%.
- Replikacja algorytmu: odtwarza 491/500 zbiorów Graph@5 z rundy; różnice wynikają z tie-breakingu `shortest_path`.
- EX tych wariantów nieznane (wymaga LLM).

**O5 – tokenizacja (`o5_tokenizacja_base64.json`)**
- Base64 kosztuje 0,94–0,97 tokenu cl100k na bajt danych binarnych (ok. 0,72 tokenu na znak).
- 1 MiB danych = 1 002 242 tokeny.
- Przykładowy kursor wzoru (10): 617 znaków, 418 tokenów. Wartości graniczne pochodzą ze wzorów generatora `seed_1m_records.py`.

**O6 – graf FK (`o6_stopnie_grafu.json`, rys. `fig_stopnie_grafu`)**
- 20 tabel o stopniu 30–33 i 980 tabel o stopniu 1–3; 86% tabel ma stopień ≤ 2.
- **Graf nie jest bezskalowy** (brak rozkładu potęgowego). Poprawiono 1.2, 4.3 i 5.1.

**O7 – model S* (`o7_model_sstar.json`, tabela `tab:oob_model`)**
- Model daje 12,85 KiB wobec 12,90 KiB [12,2; 13,5] empirycznie. To spójność na tych samych danych, a nie niezależna walidacja.
- Składnik JSON/resztowy to 44% kosztu jednostkowego In-Band.
- Z weryfikacją SHA przy odczycie SHM próbka byłaby na poziomie 14,1 KiB.

**O8 – bootstrap blokowy (`o8_lag_bootstrap_blokowy.json`)**
- Autokorelacja rzędu 1 próbek POOL: ≈ −0,02.
- 95% CI różnicy median przy M = 100: [−995; −972] ms; wniosek H3 bez zmian.
- SYNC przy M = 500 ma n = 6.

**O8b – ogon p99 (`o8b_ogon_lag.json`)**
- **100% skoków > 50 ms w POOL pokrywa się z próbką pamięci aparatury**.
- Po wyłączeniu 4% próbek p99 spada z 148 do 1,07 ms.

**O9 – przegląd błędów (`o9_proba_bledow.csv`, `o9_klasyfikacja_reczna.csv`, `o9_podsumowanie.json`)**
- Próba: 40 rekordów (po 20 na profil, ziarno 2026).
- Ekstrakcja SQL działa poprawnie (100% odpowiedzi w bloku ```sql, `done_reason = stop`).
- TABLE_MISSING oznacza w 99% błąd nieistniejącej kolumny FK (model używa tabel z pytania bez ich DDL).
- Format odpowiedzi nie wyjaśnia relacji 3B ≥ 7B.
- Usterka HAVING – patrz §4.1.

**K7 – kryterium behawioralne (`k7_llm_injection_kryterium.json`)**
- 0/26 odpowiedzi zawiera wywołanie narzędzia.
- Wszystkie „sukcesy” według regex to cytaty treści rekordu.

## 4. Błędy w kodzie i aparaturze – SPRAWY PILNE dla autora

1. **Usterka zbioru Scenariusza A (największy wpływ na opublikowane wyniki).**
   - Szablon pytań średnich (250/500) instruuje `HAVING total_count >= 1`; PostgreSQL nie dopuszcza aliasu w HAVING.
   - Model wykonuje tę instrukcję w 99,6% przypadków. W efekcie Valid SQL = 3/3500 i EX = 0 dla wszystkich strategii i obu profili.
   - Skutki: obniżone wszystkie EX, mniejsza moc porównań McNemara, część kategorii błędów to artefakt.
   - Wyniki **nie zostały poprawione**. Usterkę opisano w 6.2.1, 6.2.4 i 7.4.
   - Poprawka szablonu jest na `post-review-fixes`; wymaga nowej rundy.
2. **Pytania wymieniają wszystkie tabele** (Identifier-Match = 100%). Scenariusz A nie testuje przypadku motywującego metodę. To decyzja autora (§6b, §6c).
3. **Kursor HMAC** (wykryte testem jednostkowym, bez wpływu na wyniki pomiarowe):
   - `query_hash` nie jest weryfikowany, więc kursor z innej tabeli zostaje przyjęty;
   - akceptowany jest niepodpisany kursor starszego formatu.
   - Poprawka jest na `post-review-fixes`; w pracy opisano ją jako poprawkę po rundzie.
4. **Ogon p99 POOL** to artefakt próbnika pamięci aparatury (synchroniczny odczyt PSS w pętli zdarzeń). Wpływa tylko na interpretację p99; mediany są nienaruszone. Poprawka (`asyncio.to_thread`) jest na `post-review-fixes`.
5. **Koszt pamięci POOL zawyżony.** Proces roboczy walidatora importuje `src.mcp_server` → serwer → SchemaEngine + torch + sentence-transformers, a także tworzy model osadzeń. Poprawka (leniwy import pakietu) jest na `post-review-fixes`: import `security` nie ładuje już torch.
6. **Asymetria B1.** Ścieżka SHM w pomiarze nie weryfikowała SHA przy odczycie. Według modelu S* zaniżony o ok. 1,2 KiB. Poprawka jest na `post-review-fixes`.
7. **Test fantomów bez mocy.** Wiersz wstawiony z `created_at = NOW()` przy `ORDER BY created_at DESC` nigdy nie trafiłby na drugą stronę. Tekst osłabiono; poprawka testu nie została wykonana (wymaga zmiany projektu testu).
8. **Brak limitu migawek, `acquire_timeout = None`.** Ryzyko zawieszenia przy wielu sesjach z migawkami. Opisano w 4.2.3 i 7.4; brak poprawki.
9. **C2.** Wyścig keep-alive uvicorn (5 s) z interwałem 5 s. Poprawka `timeout_keep_alive = 30` jest na `post-review-fixes`.
10. **Predykat keyset przy NULL jest poprawny** w wersji z rundy (42 przypadki testu, SQLite w pamięci). Błąd dotyczył tylko wzoru (6) w tekście.

Testy na `post-review-fixes`: 178 + 4 przechodzą, w tym zmieniony test starszego kursora. **Poprawki nie są zweryfikowane pomiarem.**

## 5. Weryfikacje wykonane w kodzie (zamiast założeń)

- **Transport FastMCP:** `mcp.run(transport="stdio")` (`server.py`); Streamable HTTP nieużywany. Biblioteka `mcp` 2.2.0, `fastmcp` 4.0.10.
- **HTTP/2 i punkt końcowy blobów:**
  - w bramie nie ma serwera HTTP dla `/blobs/…`;
  - `OOB_HTTP_STREAM` zapisuje plik i generuje podpisany URI, a odbiór czyta plik lokalnie;
  - w pomiarze B1 całość działa w jednym procesie (bez potoku `stdio` i bez gniazda).
- **Rola bazodanowa:** `scripts/setup_runpod.sh` tworzy `mcp_user … SUPERUSER`. Scenariusz D wywołuje tylko `validate_read_only`, bez wykonania zapytań w bazie.
- **Ścieżki wywołań scenariuszy:** żaden scenariusz nie używa klienta MCP (A, TTFT, B1, B2, C1, D, dryf); wykaz w 6.1.2.
- **Importy w procesach roboczych:** potwierdzone `python -X importtime` (`security` → `server` → `schema_engine` → torch).
- **Limit migawek:** brak; `create_snapshot` czyści wygasłe migawki tylko przy tworzeniu nowej; TTL 300 s; pula `max_size = 10`.
- **Sygnalizacja błędów:**
  - odrzucenie przez AST to zwykły wynik `{"status": "błąd"}` bez `isError`;
  - wyjątki są zamieniane przez FastMCP na `isError: true`;
  - kodów −32001…−32007 z dawnej tabeli 4 nie ma w kodzie.
- **Narzędzia MCP:** `execute_read_only_query`, `execute_query_paginated`, `search_schema`, `retrieve_out_of_band_payload`, `ping_database` oraz zasoby telemetrii. Narzędzia `query_analytical_warehouse` i `discover_schema` nie istnieją; tabelę 4 i listing 3 poprawiono.
- **SSE:** w serwerze (stdio) zdarzenia Kafka są zasobami odczytywanymi na żądanie; strumień SSE istnieje tylko w bramie testowej C2.
- **Lokalny build bez biber:** magiczny komentarz `% !TEX program = xelatex` przy `enableMagicComments = true` w `.vscode/settings.json` powodował budowanie samym xelatex.
  - Plik `.vscode` jest w `.gitignore`; zmieniono go tylko lokalnie (`enableMagicComments = false`).
  - W repozytorium naprawę zapewniają `% !BIB program = biber` i `latexmkrc`.

## 6. Decyzje dla autora (gotowe teksty)

Tytułu, hipotez i pytań badawczych nie zmieniałem. Wyjątek to terminologia nazwy algorytmu w H1 i PB1 („ekspansja grafowa o tabele mostkujące” zamiast „domknięcie grafowe”) oraz wcześniejsze dopasowanie H1–H3 do projektu testów z poprzedniej aktualizacji.

### (a) Propozycje tytułu (skala: 1000 tabel, 1 mln wierszy)
1. „Analiza wydajności i bezpieczeństwa komponentów bramy pośredniczącej MCP w integracji systemów agentowych z relacyjnymi bazami danych o złożonym schemacie”
   EN: „Performance and Security Analysis of MCP Gateway Components for Integrating Agent Systems with Relational Databases with Complex Schemas”
2. „Brama pośrednicząca Model Context Protocol dla relacyjnych baz danych o złożonym schemacie: projekt, implementacja i ewaluacja”
   EN: „A Model Context Protocol Gateway for Relational Databases with Complex Schemas: Design, Implementation and Evaluation”
3. „Selekcja schematu, paginacja i bezpieczeństwo zapytań w bramie MCP łączącej agentów LLM z relacyjną bazą danych”

### (b) Hipoteza nadrzędna i PB1/H1

**Hipoteza nadrzędna, wariant 1** (zakres komponentów):
> Zastosowanie dedykowanej bramy pośredniczącej w standardzie MCP, integrującej semantyczno-grafową selekcję schematu, adaptacyjną paginację kursorową oraz wieloprocesową walidację zapytań, pozwala ograniczyć objętość kontekstu i koszt przekazania danych oraz chronić bazę danych przed badanymi klasami ataków bez degradacji responsywności pętli zdarzeń bramy pod obciążeniem.

**Hipoteza nadrzędna, wariant 2** (krótszy):
> Komponenty bramy MCP (selekcja schematu z ekspansją grafową, paginacja keyset, transfer poza pasmem i wieloprocesowy walidator AST) poprawiają czułość selekcji, koszt przekazania danych i responsywność względem rozwiązań bazowych przy zachowaniu ochrony przed badanymi atakami strukturalnymi.

**PB1/H1, wariant A** (zawężenie do zbadanego przypadku):
> PB1: W jakim stopniu ekspansja grafowa o tabele mostkujące poprawia kompletność doboru tabel dla pytań, w których encje są wskazane jawnie, przy równym budżecie kontekstu?
> H1: … istotny wzrost Table-Recall względem RRF@8 i BM25@8 dla pytań o jawnie wskazanych encjach.
> Do 7.2: „Scenariusz tabel mostkujących nienazwanych w pytaniu pozostaje niezweryfikowany.”

**PB1/H1, wariant B** (pozostawienie PB1 jak jest):
> Werdykt PB1: „odpowiedź nierozstrzygnięta dla przypadku motywującego metodę; częściowo twierdząca dla pytań z jawnie nazwanymi tabelami”.
> W tabeli 21 dla H1: „Potwierdzona dla zbioru z jawnie nazwanymi tabelami; przypadek tabel nienazwanych niezbadany”.

### (c) Przedstawienie wyniku Identifier-Match (= 100%)
- **Wariant obecnie w pracy:**
  - w 6.2.2 jako „kontrola zbioru” z wnioskiem, że selekcja w tym zbiorze jest rozwiązywalna dopasowaniem nazw;
  - w 7.4 jako pierwsze ograniczenie;
  - w tabeli 21 jako wartość kontrolna przy H1.
- **Wariant alternatywny (silniejszy):** dodać Identifier-Match jako wiersz tabeli 7 (bez EX, z adnotacją „wymaga LLM”) i do rys. Pareto (średnio 3,49 tabeli; objętość DDL do policzenia skryptem, nie szacowano).
  - Wtedy w streszczeniu: „w badanym zbiorze dopasowanie identyfikatorów osiąga pełną czułość, więc przewaga metod wyszukiwania dotyczy wyłącznie zadania bez jawnych nazw, którego nie zbadano”.

### (d) Rozszerzenie rozdziału 2 – proponowane wpisy `.bib`

Niewstawione do pracy. Przed wstawieniem dane bibliograficzne trzeba sprawdzić w źródłach.

```bibtex
@inproceedings{lei2025spider2,
  author = {Lei, Fangyu and Chen, Jixuan and Ye, Yuxiao and others},
  title = {Spider 2.0: Evaluating Language Models on Real-World Enterprise Text-to-SQL Workflows},
  booktitle = {International Conference on Learning Representations (ICLR)}, year = {2025},
  note = {arXiv:2411.07763}}
@inproceedings{kothyari2023crush4sql,
  author = {Kothyari, Mayank and Dhingra, Dhruva and Sarawagi, Sunita and Chakrabarti, Soumen},
  title = {{CRUSH4SQL}: Collective Retrieval Using Schema Hallucination For {Text2SQL}},
  booktitle = {Proceedings of EMNLP 2023}, year = {2023}}
@inproceedings{li2023resdsql,
  author = {Li, Haoyang and Zhang, Jing and Li, Cuiping and Chen, Hong},
  title = {{RESDSQL}: Decoupling Schema Linking and Skeleton Parsing for Text-to-SQL},
  booktitle = {Proceedings of the AAAI Conference on Artificial Intelligence}, volume = {37}, year = {2023}}
@misc{hou2025mcpsecurity,
  author = {Hou, Xinyi and Zhao, Yanjie and Wang, Shenao and Wang, Haoyu},
  title = {Model Context Protocol ({MCP}): Landscape, Security Threats, and Future Research Directions},
  year = {2025}, eprint = {2503.23278}, eprinttype = {arXiv}}
```

**Szkic akapitów (2.2):**
> Spider 2.0~\cite{lei2025spider2} przenosi ewaluację Text-to-SQL na schematy korporacyjne liczące setki i tysiące kolumn, w których dotychczasowe systemy osiągają znacznie niższą dokładność niż na zbiorach Spider i BIRD. Metody CRUSH4SQL~\cite{kothyari2023crush4sql} i RESDSQL~\cite{li2023resdsql} traktują selekcję schematu (schema linking) jako osobny etap: pierwsza generuje hipotetyczny schemat i odzyskuje pasujące elementy, druga uczy ranker kolumn i tabel oddzielnie od generacji szkieletu zapytania. Ekspansja grafowa proponowana w pracy jest prostszą, nieuczoną alternatywą działającą na grafie kluczy obcych.

**Szkic akapitów (3.4):**
> Hou i in.~\cite{hou2025mcpsecurity} systematyzują zagrożenia serwerów MCP, w tym zatruwanie opisów narzędzi (tool poisoning), podszywanie się pod serwery i nadmierne uprawnienia; perymetr opisany w pracy dotyczy tylko zapytań SQL i danych zwracanych z bazy, a nie integralności metadanych narzędzi.

## 7. Uwagi o stanie repozytorium

- `doc/main.pdf`:
  - przed rozpoczęciem pracy w drzewie roboczym `main` był niezacommitowany PDF zbudowany bez biber, z surowymi kluczami;
  - został zastąpiony kompletnym PDF z `make` (commit 5ac69bb).
- Nic nie zostało wypchnięte na zdalne repozytorium. Gałęzie `poprawki-recenzja` i `post-review-fixes` oraz tag `runda-20261004-kod-774b75a` są lokalne.
- `doc/rysunki/` nadal zawiera stare, nieużywane pliki (lista w `RAPORT_AKTUALIZACJI_PRACY.md` §8); nie usuwałem ich.
