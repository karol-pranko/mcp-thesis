#!/usr/bin/env bash
# =============================================================================
# run_benchmark.sh - jedyny punkt wejścia pełnej rundy pomiarowej (RunPod GPU).
#
# Zalecane uruchomienie (bez nadzoru, odporne na rozłączenie SSH):
#   tmux new -s bench './run_benchmark.sh --archive-existing'
#   # lub: nohup ./run_benchmark.sh --archive-existing > bench_nohup.log 2>&1 &
#
# Opcje:
#   --quick                 test dymny: jedna iteracja każdego scenariusza, wyniki w benchmarks/smoke_results/<znacznik>
#   --only a,b,c            uruchom tylko wskazane scenariusze
#   --skip a,b,c            pomiń wskazane scenariusze
#   --resume                pomiń scenariusze oznaczone w manifeście jako ukończone (po przerwaniu poda)
#   --archive-existing      przenieś istniejące wyniki do benchmarks/raw_results_archive/<znacznik>/ przed startem
#   --network-mode MODE     auto|netem|emulated (Scenariusz B1, domyślnie auto)
#   --llm-injection         włącz opcjonalny scenariusz llm_injection (Scenariusz D)
#   -h, --help              pomoc
#
# Scenariusze (kolejność): preflight data pagination oob concurrency streaming security drift
#                          ablation ttft_scaling schema_quality statistics plots validation
# Zmienne: RESULTS_DIR (domyślnie benchmarks/raw_results), BENCH_CONFIG, POSTGRES_*/PG*, KAFKA_BOOTSTRAP_SERVERS.
# =============================================================================
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${PROJECT_ROOT}"

MODE="full"; ONLY=""; SKIP=""; RESUME=0; ARCHIVE=0; NETWORK_MODE="auto"; LLM_INJECTION=0
usage() { sed -n '2,24p' "$0"; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick) MODE="quick"; shift ;;
        --only) ONLY="$2"; shift 2 ;;
        --skip) SKIP="$2"; shift 2 ;;
        --resume) RESUME=1; shift ;;
        --archive-existing) ARCHIVE=1; shift ;;
        --network-mode) NETWORK_MODE="$2"; shift 2 ;;
        --llm-injection) LLM_INJECTION=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Nieznana opcja: $1" >&2; usage; exit 2 ;;
    esac
done

PYTHON="${PYTHON:-${PROJECT_ROOT}/.venv/bin/python}"
CONFIG="${BENCH_CONFIG:-${PROJECT_ROOT}/benchmarks/config/benchmark_config.json}"
if [[ "${MODE}" == "quick" ]]; then
    RESULTS_DIR="${RESULTS_DIR:-${PROJECT_ROOT}/benchmarks/smoke_results/$(date +%Y%m%d_%H%M%S)}"
else
    RESULTS_DIR="${RESULTS_DIR:-${PROJECT_ROOT}/benchmarks/raw_results}"
fi
export RESULTS_DIR BENCH_MODE="${MODE}" NETWORK_MODE

# Zmienne bazy (zgodne z setup_runpod.sh)
export POSTGRES_HOST="${POSTGRES_HOST:-${PGHOST:-localhost}}"
export POSTGRES_PORT="${POSTGRES_PORT:-${PGPORT:-5432}}"
export POSTGRES_USER="${POSTGRES_USER:-${PGUSER:-mcp_user}}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-${PGPASSWORD:-mcp_secret_password}}"
export POSTGRES_DB="${POSTGRES_DB:-${PGDATABASE:-analytics_db}}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export TOKENIZERS_PARALLELISM=false PYTHONUNBUFFERED=1

log() { printf '%(%Y-%m-%dT%H:%M:%S)T [run_benchmark] %s\n' -1 "$*"; }
RM=("${PYTHON}" "${PROJECT_ROOT}/benchmarks/run_manager.py" --output-dir "${RESULTS_DIR}" --config "${CONFIG}" --mode "${MODE}")

echo "Zalecane uruchomienie bez nadzoru:  tmux new -s bench './run_benchmark.sh --archive-existing'"
log "Tryb: ${MODE}; katalog wyników: ${RESULTS_DIR}; konfiguracja: ${CONFIG}"

# Trwały wolumen RunPod
REAL_RESULTS="$(mkdir -p "${RESULTS_DIR}" && cd "${RESULTS_DIR}" && pwd -P)"
if [[ -n "${RUNPOD_POD_ID:-}" && "${REAL_RESULTS}" != /workspace/* ]]; then
    log "OSTRZEŻENIE: ${REAL_RESULTS} nie leży na trwałym wolumenie /workspace - dane zginą po zatrzymaniu poda."
fi

# Archiwizacja / ochrona istniejących wyników
if [[ "${RESUME}" -eq 0 ]]; then
    if ! "${RM[@]}" check-empty; then
        if [[ "${ARCHIVE}" -eq 1 ]]; then
            "${RM[@]}" archive
        else
            log "BŁĄD: ${RESULTS_DIR} zawiera wyniki. Użyj --archive-existing (przeniesie je do raw_results_archive/) lub --resume."
            exit 3
        fi
    fi
fi

selected() {
    local name="$1"
    if [[ -n "${ONLY}" && ",${ONLY}," != *",${name},"* ]]; then return 1; fi
    if [[ -n "${SKIP}" && ",${SKIP}," == *",${name},"* ]]; then return 1; fi
    return 0
}

INIT_ARGS=(init --network-mode "${NETWORK_MODE}")
[[ "${RESUME}" -eq 1 ]] && INIT_ARGS+=(--resume)
"${RM[@]}" "${INIT_ARGS[@]}"
"${RM[@]}" estimate

finish() {
    local status="$1"
    "${RM[@]}" finalize --status "${status}" || true
}

# Uruchamia krok z logiem (stdout+stderr z timestampami) i wpisami w manifeście.
run_step() {
    local name="$1"; shift
    if ! selected "${name}"; then log "Pominięto ${name} (--only/--skip)"; return 0; fi
    if [[ "${RESUME}" -eq 1 ]] && "${RM[@]}" is-done "${name}"; then log "Pominięto ${name} (ukończony; --resume)"; return 0; fi
    log "START ${name}"
    "${RM[@]}" scenario-start "${name}"
    local logf="${RESULTS_DIR}/logs/${name}.log"
    set +e
    "$@" 2>&1 | while IFS= read -r line; do printf '%(%Y-%m-%dT%H:%M:%S)T %s\n' -1 "${line}"; done | tee -a "${logf}"
    local rc=${PIPESTATUS[0]}
    set -e
    "${RM[@]}" scenario-end "${name}" "${rc}"
    if [[ "${rc}" -ne 0 ]]; then
        log "BŁĄD: ${name} zakończony kodem ${rc} (log: ${logf})"
        finish "failed:${name}"
        exit "${rc}"
    fi
    log "KONIEC ${name}"
}

COMMON=(--config "${CONFIG}" --mode "${MODE}" --output-dir "${RESULTS_DIR}")
SC="${PROJECT_ROOT}/benchmarks/scenarios"

data_step() {
    if "${RM[@]}" check-data; then
        echo "Tabele schematu zawierają dane."
    else
        ROWS="$("${PYTHON}" -c "import sys; sys.path.insert(0,'${PROJECT_ROOT}'); from benchmarks.common.config import load_config; print(load_config('${CONFIG}','${MODE}')['data']['rows_per_table'])")"
        SEED="$("${PYTHON}" -c "import json; print(json.load(open('${CONFIG}'))['seeds']['data_seeder'])")"
        echo "Zasilanie tabel schematu: ${ROWS} wierszy na tabelę (seed ${SEED})."
        "${PYTHON}" "${PROJECT_ROOT}/testbed/init-scripts/seed_enterprise_tables.py" --rows-per-table "${ROWS}" --seed "${SEED}" --truncate
        "${RM[@]}" check-data
    fi
}

pagination_step() {
    "${PYTHON}" "${SC}/bench_pagination_advanced.py" "${COMMON[@]}"
    "${PYTHON}" "${SC}/bench_pagination_predicate_ablation.py" "${COMMON[@]}"
}

SEC_ARGS=()
[[ "${LLM_INJECTION}" -eq 1 ]] && SEC_ARGS+=(--llm-injection)

run_step preflight "${RM[@]}" preflight
run_step data data_step
run_step pagination pagination_step
run_step oob "${PYTHON}" "${SC}/bench_oob_crossover.py" "${COMMON[@]}" --network-mode "${NETWORK_MODE}"
run_step concurrency "${PYTHON}" "${SC}/bench_concurrency.py" "${COMMON[@]}"
run_step security "${PYTHON}" "${SC}/bench_security_suite.py" "${COMMON[@]}" "${SEC_ARGS[@]}"
run_step drift "${PYTHON}" "${SC}/bench_schema_drift.py" "${COMMON[@]}"
run_step ablation "${PYTHON}" "${SC}/bench_ablation_mexp.py" "${COMMON[@]}"
run_step ttft_scaling "${PYTHON}" "${SC}/bench_ttft_scaling.py" "${COMMON[@]}"
run_step schema_quality "${PYTHON}" "${SC}/bench_schema_quality.py" "${COMMON[@]}"
run_step statistics "${PYTHON}" "${PROJECT_ROOT}/benchmarks/statistical_analysis.py" "${COMMON[@]}"
run_step plots "${PYTHON}" "${PROJECT_ROOT}/benchmarks/plot_generators/generate_thesis_plots.py" --raw-dir "${RESULTS_DIR}" --from-manifest
run_step validation "${PYTHON}" "${PROJECT_ROOT}/benchmarks/validate_run.py" "${COMMON[@]}"

"${PYTHON}" "${PROJECT_ROOT}/benchmarks/make_summary.py" "${COMMON[@]}"
finish "completed"
RUN_ID="$("${PYTHON}" -c "import json; print(json.load(open('${RESULTS_DIR}/manifest.json'))['run_id'])")"
ARCHIVE_PATH="${PROJECT_ROOT}/benchmarks/results_${RUN_ID}.tar.gz"
tar -czf "${ARCHIVE_PATH}" -C "$(dirname "${RESULTS_DIR}")" "$(basename "${RESULTS_DIR}")"
log "Gotowe. Archiwum wyników: ${ARCHIVE_PATH} ($(du -h "${ARCHIVE_PATH}" | cut -f1))"
log "Pobranie z poda: scp -P <port> root@<ip>:${ARCHIVE_PATH} ."
