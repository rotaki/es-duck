#!/usr/bin/env bash
set -euo pipefail

# Installs DuckDB CLI locally in the scripts/duckdb directory
# Usage:
#   ./scripts/install_duckdb.sh
#
# After installation, you can run DuckDB with:
#   ./scripts/duckdb/duckdb [database_file]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKDB_DIR="${SCRIPT_DIR}/duckdb"
DUCKDB_VERSION="${DUCKDB_VERSION:-1.4.3}"

echo "Installing DuckDB ${DUCKDB_VERSION}..."
echo

# Create duckdb directory if it doesn't exist
mkdir -p "${DUCKDB_DIR}"

# Check if DuckDB is already installed
if [[ -x "${DUCKDB_DIR}/duckdb" ]]; then
  INSTALLED_VERSION=$("${DUCKDB_DIR}/duckdb" --version 2>&1 | head -1 | awk '{print $2}')
  echo "DuckDB is already installed (version: ${INSTALLED_VERSION})"
  echo "Location: ${DUCKDB_DIR}/duckdb"
  echo
  echo "To reinstall, remove the directory first:"
  echo "  rm -rf ${DUCKDB_DIR}"
  echo "  ./scripts/install_duckdb.sh"
  exit 0
fi

# Detect OS and architecture
case "$(uname -s)" in
  Darwin*)
    OS="osx"
    ;;
  Linux*)
    OS="linux"
    ;;
  MINGW*|MSYS*|CYGWIN*)
    OS="windows"
    ;;
  *)
    echo "Error: Unsupported OS: $(uname -s)" >&2
    exit 1
    ;;
esac

case "$(uname -m)" in
  x86_64|amd64)
    ARCH="amd64"
    ;;
  arm64|aarch64)
    ARCH="arm64"
    ;;
  *)
    echo "Error: Unsupported architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

# Construct download URL
# DuckDB releases: https://github.com/duckdb/duckdb/releases
if [[ "$OS" == "osx" ]]; then
  if [[ "$ARCH" == "arm64" ]]; then
    PLATFORM="osx-universal"
  else
    PLATFORM="osx-universal"
  fi
  BINARY_NAME="duckdb_cli-${OS}-universal.zip"
elif [[ "$OS" == "linux" ]]; then
  PLATFORM="linux-${ARCH}"
  BINARY_NAME="duckdb_cli-${PLATFORM}.zip"
elif [[ "$OS" == "windows" ]]; then
  PLATFORM="windows-${ARCH}"
  BINARY_NAME="duckdb_cli-${PLATFORM}.zip"
fi

DOWNLOAD_URL="https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/${BINARY_NAME}"
TEMP_ZIP="/tmp/duckdb_cli.zip"

echo "Downloading DuckDB CLI from:"
echo "  ${DOWNLOAD_URL}"
echo

# Download DuckDB CLI
if ! curl -f -L -o "${TEMP_ZIP}" "${DOWNLOAD_URL}"; then
  echo "Error: Failed to download DuckDB CLI" >&2
  echo "URL: ${DOWNLOAD_URL}" >&2
  exit 1
fi

echo "Extracting DuckDB CLI..."
unzip -q -o "${TEMP_ZIP}" -d "${DUCKDB_DIR}"

# Make executable
chmod +x "${DUCKDB_DIR}/duckdb"

# Cleanup
rm -f "${TEMP_ZIP}"

# Verify installation
if [[ -x "${DUCKDB_DIR}/duckdb" ]]; then
  INSTALLED_VERSION=$("${DUCKDB_DIR}/duckdb" --version 2>&1 | head -1)
  echo
  echo "Successfully installed DuckDB!"
  echo "Version: ${INSTALLED_VERSION}"
  echo "Location: ${DUCKDB_DIR}/duckdb"
  echo
  echo "Usage examples:"
  echo "  # Start DuckDB CLI"
  echo "  ${DUCKDB_DIR}/duckdb"
  echo
  echo "  # Open a database file"
  echo "  ${DUCKDB_DIR}/duckdb mydatabase.db"
  echo
  echo "  # Execute a SQL file"
  echo "  ${DUCKDB_DIR}/duckdb mydatabase.db < query.sql"
  echo
  echo "  # Run a single query"
  echo "  ${DUCKDB_DIR}/duckdb mydatabase.db -c \"SELECT * FROM my_table LIMIT 10;\""
  echo
else
  echo "Error: Installation failed" >&2
  exit 1
fi
