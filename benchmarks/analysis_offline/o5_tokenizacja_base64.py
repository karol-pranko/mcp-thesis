"""
[O5] Tokenizacja danych Base64 enkoderem cl100k_base (obliczenie niezależne od sprzętu).

Dla kilku rozmiarów losowych danych binarnych (generator z ziarnem 2026, wielkości 1 KiB - 1 MiB)
liczy tokeny na bajt danych binarnych i na znak tekstu Base64. Liczy też koszt tokenowy przykładowego
tokenu kursora w formacie wzoru (10), wygenerowanego funkcją encode_opaque_cursor z kodu bramy
(wersja z rundy), dla porządku z Rozdziału 6 i realistycznych wartości granicznych.
Wyjście: derived/o5_tokenizacja_base64.json
"""
import base64
import datetime as dt
import json
import random
import sys
from decimal import Decimal

import tiktoken

from wspolne import ROOT, record, write_json

enc = tiktoken.get_encoding("cl100k_base")
rng = random.Random(2026)
rows = []
for size in (1024, 4096, 16384, 65536, 262144, 1048576):
    data = rng.randbytes(size)
    b64 = base64.b64encode(data).decode("ascii")
    n = len(enc.encode(b64, disallowed_special=()))
    rows.append({"bytes": size, "base64_chars": len(b64), "tokens": n,
                 "tokens_per_byte": n / size, "tokens_per_char": n / len(b64), "bytes_per_token": size / n})

# Kursor: funkcje kodu bramy w wersji z rundy (import pakietu ładuje cały serwer; tylko odczyt funkcji)
sys.path.insert(0, str(ROOT))
from src.mcp_server import pagination as pag  # noqa: E402

specs = pag.normalize_order_by(order_by="created_at DESC NULLS LAST, amount ASC NULLS LAST, id ASC")
# Wartości graniczne: ostatni wiersz pierwszej strony (K = 50) relacji analytics_orders w porządku created_at DESC,
# odtworzone wzorami deterministycznego generatora testbed/init-scripts/seed_1m_records.py
# (global_index = 1 000 000 - 50, id = global_index + 1, amount wg wzoru generatora). Znacznik czasu w generatorze
# zależy od chwili zasilania bazy (datetime.now()); przyjęto czas startu rundy z manifestu - wpływa on wyłącznie
# na cyfry znacznika, a nie na jego format i długość.
manifest = json.loads((ROOT / "results-remote-new-sunday" / "manifest.json").read_text(encoding="utf-8"))
started = dt.datetime.fromisoformat(manifest["created_at"].replace("Z", "+00:00"))
N, K = 1_000_000, 50
gi = N - K
window = 730 * 24 * 3600
created_at = started - dt.timedelta(seconds=window) + dt.timedelta(seconds=gi * window / N)
amount = Decimal(f"{(gi * 13 % 10000) + 5}.{(gi * 37 % 100):02d}")
snapshot_id = json.loads((ROOT / "results-remote-new-sunday" / "pagination_consistency.json").read_text())["snapshot_isolation"]["snapshot_id"]
safe_table = pag.sanitize_table_name("analytics_orders")  # jak w fetch_paginated_data
query_hash = __import__("hashlib").sha256(
    f"{safe_table}:{[s.column for s in specs]}:{[s.direction for s in specs]}".encode("utf-8")).hexdigest()[:8]
cursor = pag.encode_opaque_cursor(order_specs=specs, last_values=[created_at, amount, gi + 1],
                                  query_hash=query_hash, snapshot_id=snapshot_id)
cur_tokens = len(enc.encode(cursor, disallowed_special=()))
payload_b64 = cursor.split(".")[0]
payload = base64.urlsafe_b64decode(payload_b64 + "=" * (-len(payload_b64) % 4)).decode("utf-8")
out = write_json("o5_tokenizacja_base64.json", {
    "encoder": "cl100k_base",
    "random_seed": 2026,
    "base64": rows,
    "tokens_per_byte_range": [min(r["tokens_per_byte"] for r in rows), max(r["tokens_per_byte"] for r in rows)],
    "tokens_per_char_range": [min(r["tokens_per_char"] for r in rows), max(r["tokens_per_char"] for r in rows)],
    "cursor_example": {"token": cursor, "chars": len(cursor), "tokens_cl100k": cur_tokens,
                       "decoded_payload": json.loads(payload)},
})
record(__file__, [ROOT / "src" / "mcp_server" / "pagination.py", ROOT / "testbed" / "init-scripts" / "seed_1m_records.py",
                  ROOT / "results-remote-new-sunday" / "manifest.json", ROOT / "results-remote-new-sunday" / "pagination_consistency.json"],
       [out], "obliczenie niezależne od sprzętu; wartości graniczne kursora ze wzorów generatora danych")
print(json.dumps({"rows": rows, "cursor_chars": len(cursor), "cursor_tokens": cur_tokens}, indent=1))
