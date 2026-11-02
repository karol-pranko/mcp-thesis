"""
[O7] Walidacja modelu progu S* (wzór (22) w Rozdziale 4) składowymi czasów zmierzonymi w rundzie (profil loopback).

Z median składowych (oob_crossover_results.parquet) dla rozmiarów 512 B - 128 KiB (otoczenie progu) wyznacza się
metodą najmniejszych kwadratów: przepustowości v_x (nachylenie 1/v_x) dla kodowania i dekodowania Base64, SHA-256,
kopiowania do SHM i odczytu z SHM oraz składnik resztowy (e2e minus suma składowych: serializacja i parsowanie
ramki JSON, narzut wywołań), a stałe narzuty delta jako wyrazy wolne prostych e2e. Podstawienie do wzoru (22)
daje przewidywany S*, porównywany z empirycznym S* z rundy (oob_crossover_metrics.json). Dodatkowo liczony jest
wariant z weryfikacją SHA-256 przy odczycie SHM (ścieżka retrieve), której pomiar nie obejmował.
Wyjście: derived/o7_model_sstar.json
"""
import json

import numpy as np
import pandas as pd

from wspolne import RUN, record, write_json

d = pd.read_parquet(RUN / "oob_crossover_results.parquet")
l = d[(d.network_profile == "loopback") & (d.payload_size_bytes <= 131072)]
med = l.groupby(["transport_mode", "payload_size_bytes"]).median(numeric_only=True).reset_index()
emp = json.loads((RUN / "oob_crossover_metrics.json").read_text(encoding="utf-8"))["profiles"]["loopback"]["OOB_POSIX_SHM"]


def fit(x, y):
    b, a = np.polyfit(x, y, 1)
    return float(a), float(b)  # wyraz wolny [ns], nachylenie [ns/B]


ib = med[med.transport_mode == "IN_BAND_BASE64"]
sh = med[med.transport_mode == "OOB_POSIX_SHM"]
S_ib, S_sh = ib.payload_size_bytes.values, sh.payload_size_bytes.values
comp = {}
for name, frame, col in [("enc", ib, "base64_encode_ns"), ("dec", ib, "base64_decode_ns"), ("sha_ib", ib, "sha256_ns"),
                         ("sha_verify_ib", ib, "sha256_verify_ns"), ("sha_shm", sh, "sha256_ns"), ("copy", sh, "shm_copy_ns"),
                         ("read", sh, "shm_read_ns"), ("alloc", sh, "shm_alloc_ns")]:
    comp[name] = fit(frame.payload_size_bytes.values, frame[col].values)
ib_res = ib.e2e_ns - ib[["base64_encode_ns", "base64_decode_ns", "sha256_ns", "sha256_verify_ns"]].sum(axis=1)
sh_res = sh.e2e_ns - sh[["sha256_ns", "shm_alloc_ns", "shm_copy_ns", "shm_read_ns"]].sum(axis=1)
comp["resid_ib"] = fit(S_ib, ib_res.values)
comp["resid_shm"] = fit(S_sh, sh_res.values)
d_ib, c_ib = fit(S_ib, ib.e2e_ns.values)
d_sh, c_sh = fit(S_sh, sh.e2e_ns.values)
sstar_model = (d_sh - d_ib) / (c_ib - c_sh)
# wariant z weryfikacją SHA-256 przy odczycie SHM (dodatkowy składnik S / v_sha)
c_sh_verify = c_sh + comp["sha_verify_ib"][1]
sstar_model_verify = (d_sh - d_ib) / (c_ib - c_sh_verify)
mbps = lambda slope: 1e9 / slope / 2**20 if slope > 0 else None  # noqa: E731  MiB/s
res = {
    "fit_range_bytes": [int(l.payload_size_bytes.min()), int(l.payload_size_bytes.max())],
    "throughput_MiB_s": {k: mbps(v[1]) for k, v in comp.items() if k != "alloc"},
    "intercepts_us": {k: v[0] / 1000 for k, v in comp.items()},
    "delta_ib_us": d_ib / 1000, "delta_shm_us": d_sh / 1000,
    "slope_ib_ns_per_B": c_ib, "slope_shm_ns_per_B": c_sh, "denominator_positive": c_ib - c_sh > 0,
    "sstar_model_KiB": sstar_model / 1024, "sstar_model_with_shm_verify_KiB": sstar_model_verify / 1024,
    "sstar_empirical_KiB": emp["s_star_kb"], "sstar_empirical_ci_KiB": [x / 1024 for x in emp["ci95_bytes"]],
    "residual_share_of_ib_slope": comp["resid_ib"][1] / c_ib,
}
o = write_json("o7_model_sstar.json", res)
record(__file__, [RUN / "oob_crossover_results.parquet", RUN / "oob_crossover_metrics.json"], [o])
print(json.dumps(res, indent=1))
