r"""
Generuje doc/chapters/wyniki_wtorne.tex: makra LaTeX z wyników analiz wtórnych (derived/*.json).
Konwencja jak w benchmarks/analysis_final/generuj_makra.py: przecinek dziesiętny ({,}), \, jako separator
tysięcy, komentarz ze źródłem nad każdym makrem; emitowane są tylko makra użyte w tekście pracy
(kontrola: benchmarks/analysis_final/sprawdz_makra.py).
"""
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from wspolne import DERIVED, ROOT  # noqa: E402

DOC = ROOT / "doc" / "chapters"


def num(x, d=1):
    return f"{x:,.{d}f}".replace(",", "\u0000").replace(".", "{,}").replace("\u0000", "\\,")


def intf(x):
    return num(float(x), 0)


def pval(p):
    if p >= 0.001:
        return f"\\ensuremath{{{num(p, 3)}}}"
    m, e = f"{p:.1e}".split("e")
    return f"\\ensuremath{{{m.replace('.', '{,}')} \\cdot 10^{{{int(e)}}}}}"


def load(name):
    p = DERIVED / name
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


M = []


def mac(name, val, src):
    M.append((name, val, src))


o5 = load("o5_tokenizacja_base64.json")
if o5:
    s = "derived/o5_tokenizacja_base64.json (analiza wtórna, niezależna od sprzętu)"
    mac("wynOfiveTokPerByteMin", num(o5["tokens_per_byte_range"][0], 2), s + ": min tokenów cl100k_base na bajt danych binarnych")
    mac("wynOfiveTokPerByteMax", num(o5["tokens_per_byte_range"][1], 2), s + ": max tokenów na bajt")
    mac("wynOfiveTokPerChar", num(sum(o5["tokens_per_char_range"]) / 2, 2), s + ": tokenów na znak Base64 (środek zakresu)")
    mac("wynOfiveTokOneMiB", intf(o5["base64"][-1]["tokens"]), s + ": tokeny dla 1 MiB")
    mac("wynOfiveCursorChars", intf(o5["cursor_example"]["chars"]), s + ": długość przykładowego tokenu kursora")
    mac("wynOfiveCursorTokens", intf(o5["cursor_example"]["tokens_cl100k"]), s + ": tokeny przykładowego kursora")

o1 = load("o1_czulosc_a_ex.json")
if o1:
    s = "derived/o1_czulosc_a_ex.json (analiza wtórna)"
    F = o1["full_recall"]
    for key, nm in [("Graph@5", "Graph"), ("BM25@8", "BmEight"), ("RRF@8", "RrfEight"), ("BM25@5", "BmFive"), ("RRF@5", "RrfFive")]:
        mac(f"wynOoneFull{nm}", num(F[key]["full_recall_pct"]), s + f": odsetek zapytań z Table-Recall = 1, {key}")
        mac(f"wynOoneFull{nm}N", intf(F[key]["full_recall_n"]), s + f": liczba zapytań z Table-Recall = 1, {key}")
    C = o1["conditional_ex"]
    for prof, P in (("small", "Small"), ("large", "Large")):
        for key, nm in [("Graph@5", "Graph"), ("BM25@8", "BmEight"), ("RRF@8", "RrfEight")]:
            c = C[f"{prof}|{key}"]
            mac(f"wynOoneExFull{nm}{P}", num(c["ex_given_full_pct"]), s + f": EX przy pełnej czułości, {prof} {key}")
            mac(f"wynOoneExFullCi{nm}{P}", f"[{num(c['ex_given_full_ci'][0])}; {num(c['ex_given_full_ci'][1])}]", s + ": 95% CI Cloppera-Pearsona")
            mac(f"wynOoneExNotFull{nm}{P}", num(c["ex_given_not_full_pct"]), s + f": EX przy niepełnej czułości, {prof} {key}")
    B = o1["by_gt_tables"]["Graph@5"]
    for n, nm in [("2", "Two"), ("3", "Three")]:
        mac(f"wynOoneExGraphLen{nm}Small", num(B[n]["ex_small_pct"]), s + f": EX Graph@5, 3B, ścieżki {n}-tabelowe")
        mac(f"wynOoneExGraphLen{nm}Large", num(B[n]["ex_large_pct"]), s + f": EX Graph@5, 7B, ścieżki {n}-tabelowe")
        mac(f"wynOoneRecallGraphLen{nm}", num(B[n]["recall_pct"]), s + f": Table-Recall Graph@5, ścieżki {n}-tabelowe")
o2 = load("o2_holm_mcnemar.json")
if o2:
    s = "derived/o2_holm_mcnemar.json (analiza wtórna, korekta Holma w rodzinie 6 testów McNemara na profil)"
    for prof, P in (("small", "Small"), ("large", "Large")):
        for r in o2["tests"][prof]:
            for frag, nm in [("BM25@8", "BmEight"), ("RRF@8", "RrfEight"), ("DomainScoped", "Domain"), ("BM25@5", "BmFive"), ("RRF@5", "RrfFive"), ("Dense@5", "DenseFive")]:
                if frag in r["test"]:
                    mac(f"wynOtwoHolm{nm}{P}", pval(r["p_holm"]), s + ": " + r["test"])
o3 = load("o3_pareto.json")
if o3:
    s = "derived/o3_pareto.json (analiza wtórna; interpolacja liniowa między zmierzonymi punktami k)"
    for k, nm in [("BM25", "Bm"), ("RRF", "Rrf"), ("Dense", "Dense")]:
        if k in o3["recall_at_graph5_tokens_interpolated"]:
            mac(f"wynOthreeInterp{nm}", num(o3["recall_at_graph5_tokens_interpolated"][k]), s + f": czułość {k} przy liczbie tokenów DDL Graph@5")
o4 = load("o4_identifier_match.json")
if o4:
    s = "derived/o4_identifier_match.json (analiza wtórna, bez LLM i bez bazy)"
    im, gb = o4["identifier_match"], o4["graph_on_bm25"]
    mac("wynOfourImRecall", num(im["recall_pct"]), s + ": Table-Recall Identifier-Match")
    mac("wynOfourImPrecision", num(im["precision_pct"]), s + ": Table-Precision Identifier-Match")
    mac("wynOfourImFull", num(im["full_recall_pct"]), s + ": odsetek pełnej czułości Identifier-Match")
    mac("wynOfourImTables", num(im["mean_n_tables"], 2), s + ": średnia liczba tabel Identifier-Match")
    mac("wynOfourGobRecall", num(gb["recall_pct"]), s + ": Table-Recall ekspansji nałożonej na BM25@5")
    mac("wynOfourGobPrecision", num(gb["precision_pct"]), s + ": Table-Precision ekspansji na BM25@5")
    mac("wynOfourGobFull", num(gb["full_recall_pct"]), s + ": pełna czułość ekspansji na BM25@5")
    mac("wynOfourGobTables", num(gb["mean_n_tables"], 2), s + ": średnia liczba tabel ekspansji na BM25@5")
    mac("wynOfourReplica", intf(gb["replication_check_rrf5_to_graph5"]["matched"]), s + ": zgodność replikacji ekspansji z Graph@5 (na 500)")
o6 = load("o6_stopnie_grafu.json")
if o6:
    s = "derived/o6_stopnie_grafu.json (analiza wtórna, deterministyczny generator schematu)"
    h = {int(k): v for k, v in o6["histogram"].items()}
    mac("wynOsixHubs", intf(sum(v for k, v in h.items() if k >= 15)), s + ": liczba tabel o stopniu >= 15")
    mac("wynOsixHubMin", intf(min(k for k in h if k >= 15)), s + ": min stopień tabel centralnych")
    mac("wynOsixHubMax", intf(o6["degree_max"]), s + ": max stopień")
    mac("wynOsixPeriph", intf(sum(v for k, v in h.items() if k < 15)), s + ": liczba pozostałych tabel")
    mac("wynOsixPeriphMax", intf(max(k for k in h if k < 15)), s + ": max stopień pozostałych tabel")
    mac("wynOsixLeTwo", num(o6["deg_le_2_pct"]), s + ": odsetek tabel o stopniu <= 2")
o7 = load("o7_model_sstar.json")
if o7:
    s = "derived/o7_model_sstar.json (analiza wtórna, MNK na medianach składowych 512 B - 128 KiB)"
    mac("wynOsevenModel", num(o7["sstar_model_KiB"], 2), s + ": S* z modelu (22)")
    mac("wynOsevenModelVerify", num(o7["sstar_model_with_shm_verify_KiB"], 1), s + ": S* z modelu z weryfikacją SHA w SHM")
    mac("wynOsevenResidShare", num(100 * o7["residual_share_of_ib_slope"]), s + ": udział składnika resztowego w koszcie jednostkowym In-Band [%]")
    for k, nm in [("enc", "Enc"), ("dec", "Dec"), ("sha_ib", "Sha"), ("copy", "Copy"), ("read", "Read"), ("resid_ib", "Json")]:
        mac(f"wynOsevenV{nm}", intf(o7["throughput_MiB_s"][k]), s + f": przepustowość {k} [MiB/s]")
    mac("wynOsevenDeltaIb", num(o7["delta_ib_us"], 1), s + ": stały narzut In-Band [us]")
    mac("wynOsevenDeltaShm", num(o7["delta_shm_us"], 1), s + ": stały narzut SHM [us]")
o8 = load("o8_lag_bootstrap_blokowy.json")
if o8:
    s = "derived/o8_lag_bootstrap_blokowy.json (analiza wtórna, ruchome bloki, B = 5000)"
    for m, nm in [("100", "Hundred"), ("500", "Fivehundred")]:
        r_ = o8["results"][m]
        mac(f"wynOeightCi{nm}", f"[{num(r_['diff_ci_block_ms'][0], 0)}; {num(r_['diff_ci_block_ms'][1], 0)}]", s + f": 95% CI różnicy median POOL-SYNC, M={m} [ms]")
        mac(f"wynOeightAcfPool{nm}", num(r_["acf1_pool"], 2), s + f": autokorelacja rzędu 1 próbek POOL, M={m}")
o8b = load("o8b_ogon_lag.json")
if o8b:
    s = "derived/o8b_ogon_lag.json (analiza wtórna; wykluczenie próbek sondy do 20 ms po próbce pamięci)"
    for key, nm in [("POOL|4|100", "Hundred"), ("POOL|4|500", "Fivehundred"), ("POOL|8|100", "EightHundred"), ("POOL|2|100", "TwoHundred")]:
        r_ = o8b[key]
        mac(f"wynObSpikes{nm}", intf(r_["spikes_gt50"]), s + f": próbki > 50 ms, {key}")
        mac(f"wynObSpikesNear{nm}", intf(r_["spikes_gt50_near_mem"]), s + f": w tym pokrywające się z próbką pamięci, {key}")
        mac(f"wynObPnnExcl{nm}", num(r_["p99_excl_ms"], 2), s + f": p99 po wykluczeniu [ms], {key}")
        mac(f"wynObPnnAll{nm}", intf(r_["p99_all_ms"]), s + f": p99 wszystkich próbek [ms], {key}")
        mac(f"wynObExclPct{nm}", num(100 * r_["n_excluded"] / r_["n"]), s + f": odsetek wykluczonych próbek, {key}")
        mac(f"wynObChildren{nm}", intf(r_["children_max"]), s + f": liczba procesów potomnych, {key}")
k7 = load("k7_llm_injection_kryterium.json")
if k7:
    s = "derived/k7_llm_injection_kryterium.json (analiza wtórna zapisanych odpowiedzi modelu)"
    mac("wynKsevenCallsRaw", intf(k7["raw"]["tool_calls_any"]), s + ": wywołania narzędzia, wariant surowy")
    mac("wynKsevenCallsXml", intf(k7["xml_encapsulated"]["tool_calls_any"]), s + ": wywołania narzędzia, wariant XML")
    mac("wynKsevenDestr", intf(k7["raw"]["tool_calls_destructive"] + k7["xml_encapsulated"]["tool_calls_destructive"]), s + ": wywołania destrukcyjne (oba warianty)")
    mac("wynKsevenN", intf(k7["raw"]["n"] + k7["xml_encapsulated"]["n"]), s + ": liczba odpowiedzi (oba warianty)")
o9 = load("o9_podsumowanie.json")
if o9:
    s = "derived/o9_podsumowanie.json (analiza wtórna)"
    e = o9["ex_by_difficulty_pct"]
    mac("wynOnineExSimple", num(e["simple"]), s + ": EX zapytań prostych (wszystkie strategie i profile)")
    mac("wynOnineExMedium", num(e["medium"]), s + ": EX zapytań średnich")
    mac("wynOnineExComplex", num(e["complex"]), s + ": EX zapytań złożonych")
    mac("wynOnineMediumN", intf(o9["medium_records"]), s + ": liczba rekordów zapytań średnich")
    mac("wynOnineMediumValid", intf(o9["medium_valid_sql"]), s + ": Valid SQL wśród rekordów średnich")
    mac("wynOnineHavingPct", num(o9["medium_having_alias_pct"]), s + ": odsetek zapytań średnich z aliasem w HAVING")
    mac("wynOnineTableMissingColumn", num(o9["table_missing_pg_error_column_pct"]), s + ": odsetek TABLE_MISSING z błędem kolumny w PostgreSQL")
    sc = o9["sample_causes"]
    tot = lambda key: sum(sc.get(key, {}).values())  # noqa: E731
    mac("wynOnineSampleA", intf(sum(v for k, d in sc.items() if "A" in k for v in d.values())), s + ": rekordy próby z przyczyną A")
    mac("wynOnineSampleB", intf(sum(v for k, d in sc.items() if "B" in k for v in d.values())), s + ": rekordy próby z przyczyną B")
    mac("wynOnineSampleC", intf(sum(v for k, d in sc.items() if "C" in k for v in d.values())), s + ": rekordy próby z przyczyną C")
    mac("wynOnineSampleD", intf(tot("D")), s + ": rekordy próby z przyczyną D")
    mac("wynOnineSampleE", intf(tot("E")), s + ": rekordy próby z przyczyną E")

# --- miejsce na kolejne analizy (O1-O9) dopisywane przez ten sam skrypt ---
extra = DERIVED / "makra_wtorne.json"
if extra.exists():
    for name, (val, src) in json.loads(extra.read_text(encoding="utf-8")).items():
        mac(name, val, src)

tex = [p for p in DOC.glob("*.tex") if p.name not in ("wyniki_liczby.tex", "wyniki_wtorne.tex")]
tex.append(Path(os.path.realpath(ROOT / "doc" / "main.tex")))
used = set(re.findall(r"\\(wyn[A-Za-z]+)", "\n".join(p.read_text(encoding="utf-8") for p in tex)))
lines = ["% Wygenerowano: benchmarks/analysis_offline/makra_wtorne.py -- NIE EDYTOWAĆ RĘCZNIE.",
         "% Analizy wtórne danych rundy 20261004T120209_d347d2 (results-remote-new-sunday/derived/, manifest: derived/MANIFEST.json).", ""]
seen = set()
for n, v, s in M:
    if n not in used or n in seen:
        continue
    seen.add(n)
    lines += [f"% {s}", f"\\newcommand{{\\{n}}}{{{v}}}"]
(DOC / "wyniki_wtorne.tex").write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"makra wtórne: {len(seen)} użyte z {len(M)}")
