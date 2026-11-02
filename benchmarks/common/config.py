"""
Wspólna konfiguracja rundy pomiarowej.

Konfiguracja pochodzi wyłącznie z pliku benchmarks/config/benchmark_config.json. W trybie
`quick` nakładana jest sekcja `quick_overrides` (jedna iteracja każdego scenariusza). Podstawienie
modelu profilu `large` modelem 3B jest dozwolone wyłącznie w trybie `quick` i jest zapisywane
w manifeście jako `smoke_test_model_substitution: true`; w trybie `full` powoduje przerwanie.
"""
from __future__ import annotations

import argparse
import copy
import json
import os
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "benchmarks" / "config" / "benchmark_config.json"


class ConfigError(RuntimeError):
    """Błąd konfiguracji przerywający przebieg."""


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def load_config(path: Path | str | None = None, mode: str = "full") -> Dict[str, Any]:
    """Wczytuje konfigurację i nakłada nadpisania trybu `quick`."""
    if mode not in ("full", "quick"):
        raise ConfigError(f"Nieznany tryb: {mode}")
    cfg_path = Path(path) if path else DEFAULT_CONFIG
    raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    quick = raw.pop("quick_overrides", {})
    cfg = deep_merge(raw, quick) if mode == "quick" else raw
    cfg["mode"] = mode
    cfg["config_path"] = str(cfg_path)

    substitution = cfg.get("llm", {}).pop("smoke_test_model_substitution", None)
    cfg["llm"]["smoke_test_model_substitution"] = False
    if substitution:
        if mode != "quick":
            raise ConfigError("Podstawienie modelu (smoke_test_model_substitution) jest zabronione w trybie full.")
        for profile, model in substitution.items():
            cfg["llm"]["profiles"][profile]["configured_model"] = cfg["llm"]["profiles"][profile]["model"]
            cfg["llm"]["profiles"][profile]["model"] = model
        cfg["llm"]["smoke_test_model_substitution"] = True
    if mode == "full":
        for name, prof in cfg["llm"]["profiles"].items():
            if "configured_model" in prof:
                raise ConfigError(f"Profil {name}: wykryto podstawienie modelu w trybie full.")
    return cfg


def db_params(cfg: Dict[str, Any]) -> Dict[str, Any]:
    d = cfg["database"]
    return {
        "host": os.getenv(d["host_env"], d["host_default"]),
        "port": int(os.getenv(d["port_env"], str(d["port_default"]))),
        "user": os.getenv(d["user_env"], d["user_default"]),
        "password": os.getenv(d["password_env"], d["password_default"]),
        "database": os.getenv(d["name_env"], d["name_default"]),
    }


def add_common_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Wspólne argumenty wszystkich scenariuszy uruchamianych przez run_benchmark.sh."""
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Plik konfiguracji rundy.")
    parser.add_argument("--mode", choices=["full", "quick"], default=os.getenv("BENCH_MODE", "full"))
    parser.add_argument(
        "--output-dir",
        default=os.getenv("RESULTS_DIR", str(PROJECT_ROOT / "benchmarks" / "raw_results")),
        help="Katalog wyników (RESULTS_DIR).",
    )
    return parser


def scenario_cfg(cfg: Dict[str, Any], name: str) -> Dict[str, Any]:
    return cfg["scenarios"][name]
