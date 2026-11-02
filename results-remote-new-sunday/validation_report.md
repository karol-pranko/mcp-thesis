# Raport walidacji rundy 20261004T120209_d347d2

Tryb: full

Błędy: 1, ostrzeżenia: 1, kontrole zaliczone: 80

## Błędy

- scenariusz streaming zakończony błędem (kod 1)

## Ostrzeżenia

- OOB: profile LAN/WAN zmierzone w trybie EMULATED (opóźnienie sieci ze wzoru)

## Kontrole zaliczone

- manifest.json istnieje
- environment.json istnieje
- log scenariusza pagination istnieje
- log scenariusza oob istnieje
- log scenariusza concurrency istnieje
- log scenariusza streaming istnieje
- log scenariusza security istnieje
- log scenariusza drift istnieje
- log scenariusza ablation istnieje
- log scenariusza ttft_scaling istnieje
- log scenariusza schema_quality istnieje
- log scenariusza statistics istnieje
- log scenariusza plots istnieje
- plik pagination_advanced_1m.csv istnieje
- pagination_advanced_1m: 250 rekordów (oczekiwano 250)
- pagination_advanced_1m: brak NaN w kolumnie latency_ms
- plik pagination_predicate_ablation.csv istnieje
- pagination_predicate_ablation: 200 rekordów
- pagination_explain_plans.json kompletny
- pagination_consistency.json istnieje
- plik oob_crossover_results.csv istnieje
- oob_crossover_results: 3150 rekordów (oczekiwano 3150)
- oob_crossover_results: brak NaN w kolumnie e2e_ns
- oob_crossover_results: brak NaN w kolumnie measurement_mode
- OOB: wszystkie transfery zweryfikowane SHA-256
- oob_crossover_metrics.json istnieje
- oob_model_fit.json istnieje
- oob_h2_tests.json istnieje
- plik concurrency_summary.csv istnieje
- concurrency_summary: 25 poziomów (oczekiwano 25)
- concurrency_summary: brak NaN w kolumnie requests_per_second
- concurrency_summary: brak NaN w kolumnie lag_median_ms
- concurrency_summary: brak NaN w kolumnie tree_mem_peak_mb
- C1: każdy poziom ma próbki lagu
- concurrency_lag_samples.parquet istnieje
- concurrency_h3_tests.json istnieje
- plik security_suite_results.csv istnieje
- plik security_vectors_unique.csv istnieje
- security: 77 wyników dla 77 unikalnych wektorów
- security_xml_token_overhead.csv istnieje
- schema_drift_summary.json istnieje
- plik ablation_m_exp_calls.csv istnieje
- ablation_m_exp_calls: 2500 wywołań
- ablation_m_exp_calls: brak NaN w kolumnie t_total_ns
- ablation_m_exp_calls: brak NaN w kolumnie t_bridge_search_ns
- ablation_m_exp_calls: brak NaN w kolumnie table_recall
- ablation_m_exp_sweet_spot.json istnieje
- plik ttft_scaling.parquet istnieje
- ttft_scaling: 372 wywołań (oczekiwano 372)
- ttft_scaling: brak NaN w kolumnie ttft_ms
- ttft_scaling: brak NaN w kolumnie prompt_eval_count
- plik schema_quality_generation.parquet istnieje
- schema_quality_generation: 7000 rekordów (oczekiwano 7000)
- schema_quality_generation: brak NaN w kolumnie valid_sql
- schema_quality_generation: brak NaN w kolumnie ex_match
- schema_quality_generation: brak NaN w kolumnie error_category
- schema_quality_generation: brak NaN w kolumnie prompt_eval_count
- schema_quality_generation: brak NaN w kolumnie eval_count
- taksonomia: każdy rekord EX=False ma kategorię błędu
- taksonomia: rekordy EX=True mają kategorię SUCCESS
- plik schema_error_decomposition.csv istnieje
- taksonomia: suma kategorii = liczba EX=False
- brak zapytań GT z pustym wynikiem
- plik schema_retrieval.parquet istnieje
- schema_retrieval: brak NaN w kolumnie table_recall
- schema_retrieval: brak NaN w kolumnie table_precision
- schema_retrieval: brak NaN w kolumnie ddl_tokens
- schema_retrieval: brak NaN w kolumnie prompt_tokens
- schema_quality_dataset.jsonl istnieje
- statistical_hypothesis_tests.csv istnieje
- statistical_bootstrap_ci.csv istnieje
- statistical_report.json istnieje
- wykres fig_schema_recall_precision.pdf istnieje
- wykres fig_text2sql_accuracy.pdf istnieje
- wykres fig_error_decomposition_tree.pdf istnieje
- wykres fig_ablation_mexp.pdf istnieje
- wykres fig_ttft_scaling.pdf istnieje
- wykres fig_oob_crossover_rtt.pdf istnieje
- wykres fig_pagination_depth_1m.pdf istnieje
- wykres fig_concurrency_rps_lag.pdf istnieje
