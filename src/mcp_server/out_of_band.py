#!/usr/bin/env python3
"""
Transfer Out-of-Band: duże wyniki nie idą przez JSON-RPC, tylko bokiem.

- IN_BAND        - base64 w odpowiedzi (małe ładunki)
- OOB_POSIX_SHM  - segment w /dev/shm, w odpowiedzi tylko deskryptor + SHA-256
- OOB_HTTP_STREAM - plik w lokalnym magazynie blobów + presigned URI (HMAC, TTL)

Każda ścieżka zwraca też overhead_ms (delta_OOB z pracy).
"""

import atexit
import base64
from enum import Enum
import hashlib
import hmac
import json
import logging
from multiprocessing import shared_memory
import os
from pathlib import Path
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger("mcp_out_of_band")


# Do benchmarku: jak ktoś podstawi tu dict, to funkcje niżej dopisują czasy etapów w ns
# (base64_encode_ns, sha256_ns, shm_alloc_ns, ...). Normalnie None i nic się nie zbiera.
TIMING_SINK: Optional[Dict[str, int]] = None


def _tmark(key: str, t0_ns: int) -> int:
    t1 = time.perf_counter_ns()
    if TIMING_SINK is not None:
        TIMING_SINK[key] = TIMING_SINK.get(key, 0) + (t1 - t0_ns)
    return t1


class TransferMode(str, Enum):
    IN_BAND = "IN_BAND"
    OOB_POSIX_SHM = "OOB_POSIX_SHM"
    OOB_HTTP_STREAM = "OOB_HTTP_STREAM"
    AUTO = "AUTO"


# sekret do podpisu presigned URI; domyślny jest tylko na potrzeby testbedu
OOB_PRESIGNED_SECRET: bytes = os.getenv(
    "MCP_OOB_PRESIGNED_SECRET", "mcp-oob-presigned-secret-key-2026-32b"
).encode("utf-8")

DEFAULT_BLOB_DIR = Path(
    os.getenv("MCP_BLOB_STORAGE_DIR", os.path.join(tempfile.gettempdir(), "mcp_oob_blobs"))
)
DEFAULT_BLOB_DIR.mkdir(parents=True, exist_ok=True)

# segmenty, których nikt jeszcze nie odczytał z unlinkiem - sprzątamy je przy wyjściu,
# inaczej zostają w /dev/shm do restartu maszyny
_ACTIVE_SHM_SEGMENTS: Set[str] = set()


def _cleanup_all_shm() -> None:
    for shm_name in list(_ACTIVE_SHM_SEGMENTS):
        try:
            shm = shared_memory.SharedMemory(name=shm_name)
            shm.close()
            shm.unlink()
            logger.debug("Automatycznie zwolniono osierocony segment SHM: %s", shm_name)
        except Exception:
            pass
    _ACTIVE_SHM_SEGMENTS.clear()


atexit.register(_cleanup_all_shm)


def serialize_payload_to_bytes(data: Union[bytes, str, Dict[str, Any], List[Any]]) -> bytes:
    if isinstance(data, bytes):
        return data
    elif isinstance(data, str):
        return data.encode("utf-8")
    elif isinstance(data, (dict, list)):
        return json.dumps(data, ensure_ascii=False).encode("utf-8")
    else:
        return str(data).encode("utf-8")


# --- POSIX shared memory ---

def write_posix_shm(
    data_bytes: bytes,
    prefix: str = "mcp_shm_",
) -> Tuple[Dict[str, Any], float]:
    """Kopiuje dane do nowego segmentu SHM. Zwraca (deskryptor, narzut w ms)."""
    t_start = time.perf_counter_ns()

    shm_id = f"{prefix}{uuid.uuid4().hex[:12]}"
    data_len = len(data_bytes)

    # na Linuksie to po prostu plik /dev/shm/<shm_id>
    _t = time.perf_counter_ns()
    shm = shared_memory.SharedMemory(create=True, size=data_len, name=shm_id)
    _t = _tmark("shm_alloc_ns", _t)
    try:
        shm.buf[:data_len] = data_bytes
        _ACTIVE_SHM_SEGMENTS.add(shm_id)
    finally:
        shm.close()
    _t = _tmark("shm_copy_ns", _t)

    sha256_hash = hashlib.sha256(data_bytes).hexdigest()
    _tmark("sha256_ns", _t)

    descriptor: Dict[str, Any] = {
        "type": "shm",
        "transport": TransferMode.OOB_POSIX_SHM.value,
        "name": f"/dev/shm/{shm_id}",
        "shm_id": shm_id,
        "size_bytes": data_len,
        "sha256": sha256_hash,
    }

    t_end = time.perf_counter_ns()
    delta_oob_ms = (t_end - t_start) / 1_000_000.0
    descriptor["overhead_ms"] = round(delta_oob_ms, 4)

    logger.debug(
        "Zalokowano segment SHM '%s' (rozmiar: %d B, delta_OOB: %.4f ms)",
        shm_id,
        data_len,
        delta_oob_ms,
    )
    return descriptor, delta_oob_ms


def read_posix_shm(
    name_or_path: str,
    size_bytes: Optional[int] = None,
    unlink_after_read: bool = True,
) -> bytes:
    """
    size_bytes warto podać - na niektórych platformach segment jest większy niż dane
    (zaokrąglenie do strony) i bez tego na końcu przyjdą zera.
    """
    clean_name = name_or_path.replace("/dev/shm/", "").strip("/")
    _t = time.perf_counter_ns()
    shm = shared_memory.SharedMemory(name=clean_name)
    try:
        if size_bytes is not None:
            data = bytes(shm.buf[:size_bytes])
        else:
            data = bytes(shm.buf)
        _tmark("shm_read_ns", _t)
        return data
    finally:
        shm.close()
        if unlink_after_read:
            try:
                shm.unlink()
                _ACTIVE_SHM_SEGMENTS.discard(clean_name)
            except FileNotFoundError:
                pass


# --- HTTP stream (blob + presigned URI) ---

def write_http_stream_blob(
    data_bytes: bytes,
    storage_dir: Optional[Path] = None,
    server_port: int = 8080,
    ttl_seconds: int = 300,
) -> Tuple[Dict[str, Any], float]:
    """
    Zapisuje blob na dysk i składa URI podpisane HMAC-iem (blob_id:expires).
    Serwera HTTP tu nie ma - retrieve() czyta blob prosto z file_path.
    """
    t_start = time.perf_counter_ns()

    target_dir = storage_dir or DEFAULT_BLOB_DIR
    target_dir.mkdir(parents=True, exist_ok=True)

    data_len = len(data_bytes)
    _t = time.perf_counter_ns()
    sha256_hash = hashlib.sha256(data_bytes).hexdigest()
    _t = _tmark("sha256_ns", _t)
    blob_id = f"blob_{sha256_hash[:16]}"
    file_path = target_dir / f"{blob_id}.bin"

    with open(file_path, "wb") as f_out:
        f_out.write(data_bytes)
    _tmark("blob_write_ns", _t)

    expires_at = int(time.time()) + ttl_seconds
    token_payload = f"{blob_id}:{expires_at}"
    presigned_token = hmac.new(
        OOB_PRESIGNED_SECRET, token_payload.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    resource_uri = (
        f"http://127.0.0.1:{server_port}/blobs/{blob_id}"
        f"?expires={expires_at}&token={presigned_token}"
    )

    descriptor: Dict[str, Any] = {
        "type": "http_stream",
        "transport": TransferMode.OOB_HTTP_STREAM.value,
        "resource_uri": resource_uri,
        "file_path": str(file_path.resolve()),
        "blob_id": blob_id,
        "size_bytes": data_len,
        "sha256": sha256_hash,
        "expires_at": expires_at,
    }

    t_end = time.perf_counter_ns()
    delta_oob_ms = (t_end - t_start) / 1_000_000.0
    descriptor["overhead_ms"] = round(delta_oob_ms, 4)

    return descriptor, delta_oob_ms


# --- in-band (base64) ---

def write_in_band(data_bytes: bytes) -> Tuple[Dict[str, Any], float]:
    t_start = time.perf_counter_ns()

    _t = time.perf_counter_ns()
    b64_encoded = base64.b64encode(data_bytes).decode("ascii")
    _t = _tmark("base64_encode_ns", _t)
    sha256_hash = hashlib.sha256(data_bytes).hexdigest()
    _tmark("sha256_ns", _t)

    descriptor: Dict[str, Any] = {
        "type": "in_band",
        "transport": TransferMode.IN_BAND.value,
        "size_bytes": len(data_bytes),
        "sha256": sha256_hash,
        "payload": b64_encoded,
    }

    t_end = time.perf_counter_ns()
    delta_oob_ms = (t_end - t_start) / 1_000_000.0
    descriptor["overhead_ms"] = round(delta_oob_ms, 4)

    return descriptor, delta_oob_ms


class DataTransferDispatcher:
    """Wybiera ścieżkę transferu (próg dla AUTO domyślnie 64 KB)."""

    def __init__(self, threshold_bytes: int = 65536) -> None:
        self.threshold_bytes = threshold_bytes

    def dispatch(
        self,
        data: Union[bytes, str, Dict[str, Any], List[Any]],
        mode: Union[str, TransferMode] = TransferMode.AUTO,
    ) -> Dict[str, Any]:
        """
        AUTO: >= threshold_bytes idzie przez SHM, mniejsze in-band.
        Pozostałe tryby wymuszają konkretną ścieżkę. Nieznany string traktujemy jak AUTO.
        """
        raw_bytes = serialize_payload_to_bytes(data)
        data_len = len(raw_bytes)

        if isinstance(mode, str):
            try:
                selected_mode = TransferMode(mode.upper())
            except ValueError:
                selected_mode = TransferMode.AUTO
        else:
            selected_mode = mode

        if selected_mode == TransferMode.AUTO:
            if data_len >= self.threshold_bytes:
                selected_mode = TransferMode.OOB_POSIX_SHM
            else:
                selected_mode = TransferMode.IN_BAND

        logger.info(
            "Dyspozycja transferu danych: %d B, wybrana ścieżka: %s",
            data_len,
            selected_mode.value,
        )

        if selected_mode == TransferMode.OOB_POSIX_SHM:
            descriptor, _ = write_posix_shm(raw_bytes)
        elif selected_mode == TransferMode.OOB_HTTP_STREAM:
            descriptor, _ = write_http_stream_blob(raw_bytes)
        else:
            descriptor, _ = write_in_band(raw_bytes)

        return descriptor

    @staticmethod
    def retrieve(descriptor: Dict[str, Any]) -> bytes:
        """Odczyt z dowolnego deskryptora + sprawdzenie SHA-256 (jeśli jest w deskryptorze)."""
        transport = descriptor.get("transport") or descriptor.get("type")
        expected_sha = descriptor.get("sha256")

        if transport in (TransferMode.OOB_POSIX_SHM.value, "shm"):
            name = descriptor.get("name") or descriptor.get("shm_id")
            if not name:
                raise ValueError("Brak identyfikatora SHM w deskryptorze.")
            raw_bytes = read_posix_shm(
                name_or_path=name,
                size_bytes=descriptor.get("size_bytes"),
                unlink_after_read=False,  # bez unlinku, żeby ten sam deskryptor dało się odczytać ponownie
            )

        elif transport in (TransferMode.OOB_HTTP_STREAM.value, "http_stream"):
            file_path = descriptor.get("file_path")
            if not file_path or not os.path.exists(file_path):
                raise FileNotFoundError(f"Nie odnaleziono pliku blobu OOB: {file_path}")
            _t = time.perf_counter_ns()
            with open(file_path, "rb") as f_in:
                raw_bytes = f_in.read()
            _tmark("blob_read_ns", _t)

        elif transport in (TransferMode.IN_BAND.value, "in_band", "IN_BAND_BASE64"):
            payload_b64 = descriptor.get("payload")
            if not payload_b64:
                raise ValueError("Brak ładunku Base64 w deskryptorze In-Band.")
            _t = time.perf_counter_ns()
            raw_bytes = base64.b64decode(payload_b64.encode("ascii"))
            _tmark("base64_decode_ns", _t)

        else:
            raise ValueError(f"Nieobsługiwany typ transportu: {transport}")

        if expected_sha:
            _t = time.perf_counter_ns()
            actual_sha = hashlib.sha256(raw_bytes).hexdigest()
            _tmark("sha256_verify_ns", _t)
            if actual_sha != expected_sha:
                raise ValueError(
                    f"Błąd integralności SHA-256! Oczekiwano: {expected_sha}, otrzymano: {actual_sha}"
                )

        return raw_bytes


data_dispatcher = DataTransferDispatcher()
