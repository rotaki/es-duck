#!/usr/bin/env bash
set -euo pipefail

# Simple helper to set up a local ClickHouse instance for testing.
# Downloads and uses the official ClickHouse single binary.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLICKHOUSE_BIN_DIR="${SCRIPT_DIR}/clickhouse-bin"
CLICKHOUSE_DATA_DIR="${SCRIPT_DIR}/clickhouse-data"
CLICKHOUSE_LOG="${SCRIPT_DIR}/clickhouse.log"
CLICKHOUSE_PID="${SCRIPT_DIR}/clickhouse.pid"

HTTP_PORT="${HTTP_PORT:-8123}"
TCP_PORT="${TCP_PORT:-9000}"

# 1. Download ClickHouse if not present
if [[ ! -x "${CLICKHOUSE_BIN_DIR}/clickhouse" ]]; then
  echo "ClickHouse binary not found locally. Fetching latest version..."

  # Get the latest release tag from GitHub (includes "v" prefix and "-stable" suffix)
  TAG_NAME=$(curl -s "https://api.github.com/repos/ClickHouse/ClickHouse/releases/latest" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')

  if [[ -z "$TAG_NAME" ]]; then
    echo "Error: Could not fetch latest ClickHouse version" >&2
    exit 1
  fi

  echo "Downloading ${TAG_NAME}..."
  mkdir -p "${CLICKHOUSE_BIN_DIR}"

  OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
  ARCH="$(uname -m)"

  # Determine the correct binary name based on OS and architecture
  if [[ "$OS" == "darwin" ]]; then
    # macOS binaries use "macos" prefix
    if [[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]]; then
      BINARY_NAME="clickhouse-macos-aarch64"
    elif [[ "$ARCH" == "x86_64" ]]; then
      BINARY_NAME="clickhouse-macos"
    else
      echo "Unsupported macOS architecture: $ARCH" >&2
      exit 1
    fi
  elif [[ "$OS" == "linux" ]]; then
    # Linux binaries use "linux" prefix
    if [[ "$ARCH" == "x86_64" ]]; then
      BINARY_NAME="clickhouse-linux-amd64"
    elif [[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]]; then
      BINARY_NAME="clickhouse-linux-aarch64"
    else
      echo "Unsupported Linux architecture: $ARCH" >&2
      exit 1
    fi
  else
    echo "Unsupported operating system: $OS" >&2
    exit 1
  fi

  URL="https://github.com/ClickHouse/ClickHouse/releases/download/${TAG_NAME}/${BINARY_NAME}"

  echo "Downloading from ${URL}..."
  curl -f -L -o "${CLICKHOUSE_BIN_DIR}/clickhouse" "${URL}"
  chmod +x "${CLICKHOUSE_BIN_DIR}/clickhouse"
  echo "ClickHouse installed successfully!"
fi

# 2. Setup Data Directory
mkdir -p "${CLICKHOUSE_DATA_DIR}"

# 3. Start ClickHouse server
# We use 'clickhouse server' mode and pass configuration via flags to keep it portable.
if curl -s "http://localhost:${HTTP_PORT}/ping" > /dev/null; then
  echo "ClickHouse is already running on port ${HTTP_PORT}"
else
  echo "Starting ClickHouse server..."
  
  # Note: we use --daemon to run in background.
  # Configuration parameters are passed as XML-style overrides after --
  "${CLICKHOUSE_BIN_DIR}/clickhouse" server \
    --daemon \
    --pid-file="${CLICKHOUSE_PID}" \
    --log-file="${CLICKHOUSE_LOG}" \
    -- \
    --path "${CLICKHOUSE_DATA_DIR}" \
    --tmp_path "${CLICKHOUSE_DATA_DIR}/tmp" \
    --user_files_path "${CLICKHOUSE_DATA_DIR}/user_files" \
    --http_port ${HTTP_PORT} \
    --tcp_port ${TCP_PORT} \
    --logger.level information

  # Wait for ClickHouse to be ready
  echo "Waiting for ClickHouse to be ready..."
  for i in {1..30}; do
    if curl -s "http://localhost:${HTTP_PORT}/ping" | grep -q "Ok"; then
      echo "ClickHouse is ready!"
      break
    fi
    if [[ $i -eq 30 ]]; then
      echo "Error: ClickHouse failed to start. Check ${CLICKHOUSE_LOG}" >&2
      exit 1
    fi
    sleep 1
  done
fi

echo
echo "Local ClickHouse is ready."
echo "HTTP URL: http://localhost:${HTTP_PORT}"
echo "TCP Port: ${TCP_PORT}"
echo
echo "To stop ClickHouse:"
echo "  kill \$(cat ${CLICKHOUSE_PID})"