#!/usr/bin/env bash
# =============================================================================
# scripts/setup_runpod.sh
# Skrypt bootstrapu środowiska benchmarkowego na instancji RunPod.io (Ubuntu 22.04 z CUDA)
#
# Zakres automatyzacji:
# 1. Instalacja pakietów systemowych (curl, git, build-essential, python3.12, jq, tmux itp.)
# 2. Usługi zależne (PostgreSQL 16 + pgvector, Apache Kafka KRaft)
#    - Fallback: Jeśli Docker jest aktywny, uruchamia usługi przez docker compose.
#    - Ścieżka główna: Natywna instalacja usług w systemie.
# 3. Konfiguracja środowiska Python (.venv) i instalacja zależności projektu.
# 4. Zasielenie bazy danych (Seed Data):
#    - 1000 tabel w topologii bezskalowej (generate_enterprise_schema.py),
#    - Indeksy HNSW w schema_embeddings oraz relacje w schema_foreign_keys,
#    - 1 000 000 rekordów w tabeli analitycznej (seed_1m_records.py).
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"
# Przypięte wersje usług (jedno źródło prawdy)
set -a; source "${SCRIPT_DIR}/versions.env"; set +a

# Kolory i helpery logowania
RESET="\033[0m"; BOLD="\033[1m"
GREEN="\033[0;32m"; YELLOW="\033[0;33m"; RED="\033[0;31m"; CYAN="\033[0;36m"

log_info()    { echo -e "${CYAN}[INFO]${RESET}  $(date '+%H:%M:%S') $*"; }
log_ok()      { echo -e "${GREEN}[OK]${RESET}    $(date '+%H:%M:%S') $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${RESET}  $(date '+%H:%M:%S') $*"; }
log_error()   { echo -e "${RED}[ERROR]${RESET} $(date '+%H:%M:%S') $*" >&2; }
log_section() {
    echo -e "\n${BOLD}${CYAN}════════════════════════════════════════${RESET}"
    echo -e "${BOLD}${CYAN}  $*${RESET}"
    echo -e "${BOLD}${CYAN}════════════════════════════════════════${RESET}"
}

export DEBIAN_FRONTEND=noninteractive
export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-mcp_user}"
export PGPASSWORD="${PGPASSWORD:-mcp_secret_password}"
export PGDATABASE="${PGDATABASE:-analytics_db}"
export POSTGRES_HOST="${PGHOST}"
export POSTGRES_PORT="${PGPORT}"
export POSTGRES_USER="${PGUSER}"
export POSTGRES_PASSWORD="${PGPASSWORD}"
export POSTGRES_DB="${PGDATABASE}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export KAFKA_TOPIC="telemetry_test"

# -----------------------------------------------------------------------------
# 1. Pakiety systemowe
# -----------------------------------------------------------------------------
log_section "KROK 1: Instalacja pakietów systemowych"

log_info "Aktualizacja repozytoriów apt..."
apt-get update -qq

log_info "Instalacja narzędzi bazowych: curl, git, build-essential, python3.12, jq, tmux..."
apt-get install -y -qq \
    curl \
    git \
    build-essential \
    python3.12 \
    python3.12-venv \
    python3.12-dev \
    python3-pip \
    jq \
    tmux \
    ca-certificates \
    lsb-release \
    pciutils \
    lshw \
    wget \
    tar \
    2>/dev/null || {
        log_warn "Standardowa instalacja zgłosiła ostrzeżenia, instalowanie wariantu fallback..."
        apt-get install -y -qq curl git build-essential python3 python3-venv python3-dev python3-pip jq tmux ca-certificates
    }

log_ok "Pakiety systemowe zainstalowane."

# -----------------------------------------------------------------------------
# 2. Usługi zależne (Docker vs Natywny PostgreSQL + Kafka KRaft)
# -----------------------------------------------------------------------------
log_section "KROK 2: Konfiguracja usług zależnych"

DOCKER_AVAILABLE=0
if command -v docker &>/dev/null && docker info &>/dev/null; then
    DOCKER_AVAILABLE=1
fi

if [[ "${DOCKER_AVAILABLE}" -eq 1 ]]; then
    log_info "Wykryto aktywny demon Docker. Uruchamianie usług przez docker compose (obrazy: ${DOCKER_PGVECTOR_IMAGE}, ${DOCKER_KAFKA_IMAGE})..."
    cd "${PROJECT_ROOT}/testbed"
    docker compose up -d
    cd "${PROJECT_ROOT}"
    log_ok "Usługi Docker zostały uruchomione."
else
    log_info "Brak demona Docker. Konfiguracja natywna (PostgreSQL 16 + pgvector, Apache Kafka KRaft)..."

    # --- 2A. PostgreSQL 16 + pgvector ---
    log_info "Konfiguracja oficjalnego repozytorium PostgreSQL PGDG..."
    apt-get install -y -qq gnupg lsb-release curl 2>/dev/null || true
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg --yes 2>/dev/null || true
    # Repozytorium bieżące oraz archiwum PGDG (zawiera starsze wydania, potrzebne do przypięcia PG_MINOR)
    echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
    echo "deb https://apt-archive.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg-archive main" > /etc/apt/sources.list.d/pgdg-archive.list
    apt-get update -qq

    log_info "Instalacja PostgreSQL ${PG_MINOR} (przypięta wersja pakietu)..."
    PG_PKG_VERSION="$(apt-cache madison "postgresql-${PG_MAJOR}" | awk '{print $3}' | grep -E "^${PG_MINOR//./\\.}-" | head -1 || true)"
    if [[ -z "${PG_PKG_VERSION}" ]]; then
        log_error "Brak pakietu postgresql-${PG_MAJOR} w wersji ${PG_MINOR} w repozytoriach PGDG. Zaktualizuj PG_MINOR w scripts/versions.env."
        exit 1
    fi
    apt-get install -y -qq "postgresql-${PG_MAJOR}=${PG_PKG_VERSION}" "postgresql-client-${PG_MAJOR}=${PG_PKG_VERSION}" \
        "postgresql-server-dev-${PG_MAJOR}=${PG_PKG_VERSION}" libpq-dev
    apt-mark hold "postgresql-${PG_MAJOR}" "postgresql-client-${PG_MAJOR}" "postgresql-server-dev-${PG_MAJOR}"
    log_ok "PostgreSQL pakiet ${PG_PKG_VERSION}"

    # Wykrywanie katalogu konfiguracji PostgreSQL
    PG_VERSION=$(pg_lsclusters -h 2>/dev/null | awk 'NR==1{print $1}' || echo "16")
    PG_CLUSTER=$(pg_lsclusters -h 2>/dev/null | awk 'NR==1{print $2}' || echo "main")
    PG_CONF_DIR="/etc/postgresql/${PG_VERSION}/${PG_CLUSTER}"
    PG_CONF="${PG_CONF_DIR}/postgresql.conf"
    PG_HBA="${PG_CONF_DIR}/pg_hba.conf"

    log_info "Konfiguracja parametrów w ${PG_CONF} (shared_buffers=2GB, work_mem=64MB, statement_timeout=10000ms)..."
    if [[ -f "${PG_CONF}" ]]; then
        # Aktualizacja lub dopisanie parametrów
        sed -i "s/^[# ]*shared_buffers\s*=.*/shared_buffers = '2GB'/" "${PG_CONF}" || echo "shared_buffers = '2GB'" >> "${PG_CONF}"
        sed -i "s/^[# ]*work_mem\s*=.*/work_mem = '64MB'/" "${PG_CONF}" || echo "work_mem = '64MB'" >> "${PG_CONF}"
        sed -i "s/^[# ]*statement_timeout\s*=.*/statement_timeout = '10000ms'/" "${PG_CONF}" || echo "statement_timeout = '10000ms'" >> "${PG_CONF}"
    fi

    # Zezwolenie na połączenia lokalne w pg_hba.conf
    if [[ -f "${PG_HBA}" ]]; then
        cat > "${PG_HBA}" <<EOF_HBA
local   all             postgres                                trust
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
EOF_HBA
    fi

    log_info "Uruchamianie usługi PostgreSQL..."
    pg_ctlcluster "${PG_VERSION}" "${PG_CLUSTER}" start 2>/dev/null || service postgresql start 2>/dev/null || true

    for _i in $(seq 1 20); do
        pg_isready -h localhost -p 5432 -q 2>/dev/null && break
        sleep 2
    done

    # Konfiguracja użytkownika i bazy danych
    psql -U postgres -d postgres -tc "SELECT 1 FROM pg_roles WHERE rolname='${PGUSER}'" | grep -q 1 || \
        psql -U postgres -d postgres -c "CREATE USER ${PGUSER} WITH PASSWORD '${PGPASSWORD}' SUPERUSER;"
    psql -U postgres -d postgres -c "ALTER USER ${PGUSER} WITH PASSWORD '${PGPASSWORD}';"

    psql -U postgres -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='${PGDATABASE}'" | grep -q 1 || \
        psql -U postgres -d postgres -c "CREATE DATABASE ${PGDATABASE} OWNER ${PGUSER};"

    # Wymuszenie parametrów w silniku bazy
    psql -U postgres -d "${PGDATABASE}" -c "ALTER SYSTEM SET shared_buffers = '2GB';"
    psql -U postgres -d "${PGDATABASE}" -c "ALTER SYSTEM SET work_mem = '64MB';"
    psql -U postgres -d "${PGDATABASE}" -c "ALTER SYSTEM SET statement_timeout = '10000ms';"

    # pgvector: kompilacja z przypiętego tagu (wersja niezależna od repozytoriów apt)
    INSTALLED_VEC="$(psql -U postgres -d "${PGDATABASE}" -tAc "SELECT default_version FROM pg_available_extensions WHERE name='vector'" 2>/dev/null || true)"
    if [[ "${INSTALLED_VEC}" != "${PGVECTOR_VERSION}" ]]; then
        log_info "Kompilacja pgvector v${PGVECTOR_VERSION} ze źródeł..."
        TMP_VEC="/tmp/pgvector_build"
        rm -rf "${TMP_VEC}"
        git clone --depth=1 --branch "v${PGVECTOR_VERSION}" https://github.com/pgvector/pgvector.git "${TMP_VEC}"
        make -C "${TMP_VEC}" PG_CONFIG="/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config" -j"$(nproc)"
        make -C "${TMP_VEC}" PG_CONFIG="/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config" install
        rm -rf "${TMP_VEC}"
    fi
    psql -U postgres -d "${PGDATABASE}" -c "CREATE EXTENSION IF NOT EXISTS vector VERSION '${PGVECTOR_VERSION}';" || \
        psql -U postgres -d "${PGDATABASE}" -c "ALTER EXTENSION vector UPDATE TO '${PGVECTOR_VERSION}';"
    log_ok "PostgreSQL 16 + pgvector skonfigurowany pomyślnie."

    # --- 2B. Apache Kafka w trybie KRaft ---
    log_info "Instalacja Java JRE (openjdk-17-jre-headless)..."
    apt-get install -y -qq openjdk-17-jre-headless

    KAFKA_DIR="/opt/kafka"
    if [[ ! -d "${KAFKA_DIR}/bin" ]]; then
        log_info "Pobieranie Apache Kafka ${KAFKA_VERSION}..."
        KAFKA_TGZ="/tmp/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"
        curl -fsSL -o "${KAFKA_TGZ}" "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"
        mkdir -p "${KAFKA_DIR}"
        tar -xzf "${KAFKA_TGZ}" -C "${KAFKA_DIR}" --strip-components=1
        rm -f "${KAFKA_TGZ}"
    fi

    # Formatowanie storage Apache Kafka w trybie KRaft (jeśli jeszcze nie sformatowano)
    KAFKA_LOG_DIRS=$(grep "^log.dirs=" "${KAFKA_DIR}/config/kraft/server.properties" 2>/dev/null | cut -d'=' -f2 || echo "/tmp/kraft-combined-logs")
    if [[ ! -f "${KAFKA_LOG_DIRS}/meta.properties" && ! -f "/tmp/kraft-combined-logs/meta.properties" ]]; then
        log_info "Formatowanie storage Apache Kafka w trybie KRaft..."
        CLUSTER_ID=$("${KAFKA_DIR}/bin/kafka-storage.sh" random-uuid 2>/dev/null || "${KAFKA_DIR}/bin/kafka-storage.sh" random-cluster-id 2>/dev/null || echo "MkU3OEVBNTcwNTJENDM2Qk")
        "${KAFKA_DIR}/bin/kafka-storage.sh" format -t "${CLUSTER_ID}" -c "${KAFKA_DIR}/config/kraft/server.properties" --ignore-formatted || true
    fi

    # Uruchomienie brokera Kafka w tle, jeśli nie działa
    if ! (echo > /dev/tcp/localhost/9092) &>/dev/null; then
        log_info "Uruchamianie Apache Kafka w tle..."
        nohup "${KAFKA_DIR}/bin/kafka-server-start.sh" "${KAFKA_DIR}/config/kraft/server.properties" > /var/log/kafka.log 2>&1 &
        sleep 5
    fi

    # Utworzenie tematu telemetry_test z 3 partycjami
    log_info "Tworzenie tematu '${KAFKA_TOPIC}' z 3 partycjami..."
    for _k in $(seq 1 15); do
        if "${KAFKA_DIR}/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list &>/dev/null; then
            break
        fi
        sleep 2
    done
    "${KAFKA_DIR}/bin/kafka-topics.sh" --create --if-not-exists \
        --topic "${KAFKA_TOPIC}" \
        --partitions 3 \
        --replication-factor 1 \
        --bootstrap-server localhost:9092 2>/dev/null || true

    log_ok "Apache Kafka w trybie KRaft (temat '${KAFKA_TOPIC}', 3 partycje) gotowa."
fi

# --- 2C. Usługa inferencji LLM (Ollama) ---
log_section "KROK 2C: Weryfikacja serwera LLM (Ollama)"
if ! command -v ollama &>/dev/null || ! ollama --version 2>/dev/null | grep -q "${OLLAMA_VERSION}"; then
    log_info "Instalacja serwera Ollama ${OLLAMA_VERSION}..."
    curl -fsSL https://ollama.com/install.sh | OLLAMA_VERSION="${OLLAMA_VERSION}" sh
fi
# Równoległość serwera dopasowana do scenarios.schema_quality.llm_concurrency; log serwera na trwałym wolumenie
export OLLAMA_NUM_PARALLEL="${OLLAMA_NUM_PARALLEL:-4}" OLLAMA_KEEP_ALIVE="${OLLAMA_KEEP_ALIVE:-30m}"
OLLAMA_LOG="${OLLAMA_LOG:-/workspace/ollama_server.log}"
mkdir -p "$(dirname "${OLLAMA_LOG}")" 2>/dev/null || OLLAMA_LOG=/var/log/ollama.log
if ! curl -sf http://localhost:11434/api/tags &>/dev/null; then
    log_info "Uruchamianie serwera Ollama w tle (log: ${OLLAMA_LOG})..."
    nohup ollama serve >> "${OLLAMA_LOG}" 2>&1 &
    for _i in $(seq 1 30); do curl -sf http://localhost:11434/api/tags &>/dev/null && break; sleep 1; done
fi
for MODEL in "${LLM_MODEL_SMALL}" "${LLM_MODEL_LARGE}"; do
    log_info "Pobieranie modelu ${MODEL}..."
    ollama pull "${MODEL}"
done
log_ok "Ollama $(ollama --version | tail -1); modele: ${LLM_MODEL_SMALL}, ${LLM_MODEL_LARGE}."

# -----------------------------------------------------------------------------
# 3. Środowisko Python (.venv)
# -----------------------------------------------------------------------------
log_section "KROK 3: Środowisko Python i zależności"

if ! command -v uv &>/dev/null; then
    log_info "Instalacja uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="${HOME}/.local/bin:${HOME}/.cargo/bin:${PATH}"
fi

if [[ -f "pyproject.toml" ]]; then
    log_info "Synchronizacja środowiska przez uv sync..."
    uv sync --no-dev --frozen
    PYTHON="${PROJECT_ROOT}/.venv/bin/python"
else
    if [[ ! -d ".venv" ]]; then
        python3.12 -m venv .venv
    fi
    PYTHON="${PROJECT_ROOT}/.venv/bin/python"
    "${PYTHON}" -m pip install --upgrade pip
    if [[ -f "requirements.txt" ]]; then
        "${PYTHON}" -m pip install -r requirements.txt
    fi
fi

log_ok "Środowisko Python przygotowane: $("${PYTHON}" --version)"

# -----------------------------------------------------------------------------
# 4. Zasielenie bazy danych (Seed Data)
# -----------------------------------------------------------------------------
log_section "KROK 4: Inicjalizacja i zasielenie bazy danych"

SCHEMA_STAMP="${PROJECT_ROOT}/.schema_initialized"

# Sprawdzenie czy baza już zawiera wymagane dane
TABLES_COUNT=$(PGPASSWORD="${PGPASSWORD}" psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d "${PGDATABASE}" \
    -t -c "SELECT COUNT(*) FROM schema_embeddings;" 2>/dev/null | tr -d ' ' || echo "0")
ORDERS_COUNT=$(PGPASSWORD="${PGPASSWORD}" psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d "${PGDATABASE}" \
    -t -c "SELECT COUNT(*) FROM analytics_orders;" 2>/dev/null | tr -d ' ' || echo "0")

if [[ "${TABLES_COUNT}" -eq 1000 && "${ORDERS_COUNT}" -ge 1000000 ]]; then
    log_ok "Baza danych zawiera już 1000 tabel i >= 1M rekordów (schema_embeddings: ${TABLES_COUNT}, analytics_orders: ${ORDERS_COUNT}). Pomijanie seedu."
    touch "${SCHEMA_STAMP}"
else
    log_info "Generowanie korporacyjnego schematu bezskalowego (1000 tabel, relacje FK, indeksy HNSW)..."
    "${PYTHON}" "${PROJECT_ROOT}/testbed/init-scripts/generate_enterprise_schema.py"

    log_info "Generowanie 1 000 000 rekordów w tabeli analytics_orders..."
    "${PYTHON}" "${PROJECT_ROOT}/testbed/init-scripts/seed_1m_records.py"

    touch "${SCHEMA_STAMP}"
    log_ok "Inicjalizacja i zasielenie bazy zakończone pomyślnie."
fi

log_section "PODSUMOWANIE BOOTSTRAPU"
log_ok "Wszystkie komponenty infrastruktury RunPod.io zostały pomyślnie skonfigurowane."
log_info "Gotowość możesz zweryfikować za pomocą: python scripts/preflight_check.py"
