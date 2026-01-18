#!/usr/bin/env bash
set -euo pipefail

# Simple helper to set up a local Postgres database for testing
# Downloads and uses a local PostgreSQL 18 binary
#
# Usage:
#   ./scripts/run_postgres_local_test.sh
# Then, in another terminal:
#   export POSTGRES_TEST_URL=postgres://postgres@localhost:5433/es_duck_test
#   cargo test --test postgres_integration_test
#
# When you're done:
#   kill $(cat scripts/postgres-data/postmaster.pid | head -1)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POSTGRES_DIR="${SCRIPT_DIR}/postgres"
POSTGRES_DATA_DIR="${SCRIPT_DIR}/postgres-data"
POSTGRES_LOG="${SCRIPT_DIR}/postgres.log"
POSTGRES_VERSION="18.1"

POSTGRES_PORT="${POSTGRES_PORT:-5433}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-es_duck_test}"
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"

# Download PostgreSQL if not already present
if [[ ! -x "${POSTGRES_DIR}/bin/postgres" ]]; then
  echo "PostgreSQL not found locally. Downloading PostgreSQL ${POSTGRES_VERSION}..."

  # Detect OS and architecture
  case "$(uname -s)" in
    Darwin*)
      OS="macos"
      ;;
    Linux*)
      OS="linux"
      ;;
    *)
      echo "Error: Unsupported OS: $(uname -s)" >&2
      exit 1
      ;;
  esac

  case "$(uname -m)" in
    x86_64|amd64)
      ARCH="x86_64"
      ;;
    arm64|aarch64)
      ARCH="arm64"
      ;;
    *)
      echo "Error: Unsupported architecture: $(uname -m)" >&2
      exit 1
      ;;
  esac

  if [[ "$OS" == "macos" ]]; then
    # For macOS: Download binaries from Postgres.app
    echo "Downloading PostgreSQL binaries from Postgres.app..."

    # Postgres.app provides pre-built binaries in their GitHub releases
    POSTGRES_APP_VERSION="2.8.1"

    if [[ "$ARCH" == "arm64" ]]; then
      # ARM64 build
      BINARY_URL="https://github.com/PostgresApp/PostgresApp/releases/download/v${POSTGRES_APP_VERSION}/Postgres-${POSTGRES_APP_VERSION}-18-arm64.zip"
    else
      # Intel build
      BINARY_URL="https://github.com/PostgresApp/PostgresApp/releases/download/v${POSTGRES_APP_VERSION}/Postgres-${POSTGRES_APP_VERSION}-18.zip"
    fi

    BINARY_FILE="/tmp/postgres-${ARCH}.zip"

    echo "Downloading from ${BINARY_URL}..."
    if curl -f -L -o "${BINARY_FILE}" "${BINARY_URL}"; then
      echo "Extracting binaries..."
      cd /tmp
      unzip -q "${BINARY_FILE}"

      # Extract the .app bundle and copy binaries
      if [[ -d "Postgres.app" ]]; then
        # Copy the PostgreSQL binaries from the app bundle
        cp -R "Postgres.app/Contents/Versions/18/"* "${POSTGRES_DIR}/"
        rm -rf "Postgres.app"
      fi

      rm "${BINARY_FILE}"
      cd "${SCRIPT_DIR}"

      if [[ -x "${POSTGRES_DIR}/bin/postgres" ]]; then
        echo "Successfully installed PostgreSQL binaries!"
      else
        echo "Binary extraction failed, falling back to source compilation..."
        BINARY_FAILED=1
      fi
    else
      echo "Binary download failed, falling back to source compilation..."
      BINARY_FAILED=1
    fi

    # Fall back to source if binary download failed
    if [[ -n "${BINARY_FAILED}" ]]; then
      SOURCE_URL="https://ftp.postgresql.org/pub/source/v${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.tar.gz"
      SOURCE_FILE="/tmp/postgresql-${POSTGRES_VERSION}.tar.gz"

      echo "Downloading and compiling from source..."
      curl -L -o "${SOURCE_FILE}" "${SOURCE_URL}"
      tar -xzf "${SOURCE_FILE}" -C /tmp
      cd "/tmp/postgresql-${POSTGRES_VERSION}"
      ./configure --prefix="${POSTGRES_DIR}" --without-readline --without-zlib --without-icu
      make -j$(sysctl -n hw.ncpu)
      make install
      rm -rf "/tmp/postgresql-${POSTGRES_VERSION}" "${SOURCE_FILE}"
      cd "${SCRIPT_DIR}"
    fi

  else
    # For Linux: Compile from source (most reliable for non-packaged install)
    echo "Compiling PostgreSQL from source for Linux..."
    SOURCE_URL="https://ftp.postgresql.org/pub/source/v${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.tar.gz"
    SOURCE_FILE="/tmp/postgresql-${POSTGRES_VERSION}.tar.gz"

    curl -L -o "${SOURCE_FILE}" "${SOURCE_URL}"
    tar -xzf "${SOURCE_FILE}" -C /tmp
    cd "/tmp/postgresql-${POSTGRES_VERSION}"
    ./configure --prefix="${POSTGRES_DIR}" --without-readline --without-zlib --without-icu
    make -j$(nproc)
    make install
    rm -rf "/tmp/postgresql-${POSTGRES_VERSION}" "${SOURCE_FILE}"
    cd "${SCRIPT_DIR}"
  fi

  echo "PostgreSQL ${POSTGRES_VERSION} installed successfully!"
  echo
fi

# Add postgres bin directory to PATH for this script
export PATH="${POSTGRES_DIR}/bin:${PATH}"

# Initialize data directory if it doesn't exist
if [[ ! -d "${POSTGRES_DATA_DIR}" ]]; then
  echo "Initializing PostgreSQL data directory..."
  initdb -D "${POSTGRES_DATA_DIR}" -U "${POSTGRES_USER}" --no-locale --encoding=UTF8
  echo "Data directory initialized."
fi

# Check if PostgreSQL is already running
if pg_isready -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" >/dev/null 2>&1; then
  echo "PostgreSQL is already running on ${POSTGRES_HOST}:${POSTGRES_PORT}"
else
  echo "Starting PostgreSQL server on port ${POSTGRES_PORT}..."
  pg_ctl -D "${POSTGRES_DATA_DIR}" -l "${POSTGRES_LOG}" \
    -o "-p ${POSTGRES_PORT} -k ${SCRIPT_DIR} -c max_worker_processes=128 -c max_parallel_workers=128 -c max_connections=200" \
    start

  # Wait for PostgreSQL to be ready
  echo "Waiting for PostgreSQL to be ready..."
  for i in {1..30}; do
    if pg_isready -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" >/dev/null 2>&1; then
      echo "PostgreSQL is ready!"
      break
    fi
    if [[ $i -eq 30 ]]; then
      echo "Error: PostgreSQL failed to start. Check ${POSTGRES_LOG} for details." >&2
      exit 1
    fi
    sleep 1
  done
fi

echo

# Check if database exists
if psql -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -lqt | cut -d \| -f 1 | grep -qw "${POSTGRES_DB}"; then
  echo "Database '${POSTGRES_DB}' already exists."
else
  echo "Creating database '${POSTGRES_DB}'..."
  createdb -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" "${POSTGRES_DB}"
  echo "Database '${POSTGRES_DB}' created."
fi

POSTGRES_TEST_URL="postgres://${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"

echo
echo "Local Postgres database is ready."
echo "Use this in another terminal:"
echo "  export POSTGRES_TEST_URL=\"${POSTGRES_TEST_URL}\""
echo "  cargo test --test postgres_integration_test"
echo
echo "When finished, stop the PostgreSQL server with:"
echo "  ${POSTGRES_DIR}/bin/pg_ctl -D ${POSTGRES_DATA_DIR} stop"
