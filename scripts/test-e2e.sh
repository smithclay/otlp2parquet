#!/bin/bash
set -euo pipefail

#
# End-to-end Docker integration test orchestrator
#
# Usage:
#   ./scripts/test-e2e.sh                  # Run core tests only
#   TEST_ICEBERG=1 ./scripts/test-e2e.sh   # Run core + Iceberg tests
#   KEEP_CONTAINERS=1 ./scripts/test-e2e.sh # Preserve containers for debugging
#

# Check Docker availability
if ! command -v docker &> /dev/null; then
  echo "⚠️  Skipping e2e tests (docker command not found)"
  exit 0
fi

if ! docker compose version &> /dev/null; then
  echo "⚠️  Skipping e2e tests (docker compose not available)"
  exit 0
fi

# Cleanup function
cleanup() {
  if [ "${KEEP_CONTAINERS:-0}" = "1" ]; then
    echo "ℹ️  Containers preserved for debugging (KEEP_CONTAINERS=1)"
    echo "    View logs:     docker compose logs"
    echo "    Inspect MinIO: docker compose exec minio mc ls minio/otlp"
    echo "    Cleanup:       docker compose down -v"
  else
    echo "Cleaning up containers..."
    docker compose down -v
  fi
}

# Register cleanup trap
trap cleanup EXIT

# Start services
echo "Starting Docker services..."
docker compose up -d minio rest otlp2parquet

# Wait for services to be ready
echo "Waiting for otlp2parquet to be ready..."
timeout 60 bash -c 'until curl -sf http://localhost:4318/health > /dev/null 2>&1; do sleep 2; done' || {
  echo "ERROR: otlp2parquet failed to become ready within 60 seconds"
  docker compose logs otlp2parquet
  exit 1
}

echo "Services ready!"

# Run tests
echo "Running tests..."
if [ "${TEST_ICEBERG:-0}" = "1" ]; then
  echo "Running core + Iceberg tests..."
  cargo test --test e2e_docker --features docker-tests -- --test-threads=1
else
  echo "Running core tests only (skip Iceberg)..."
  cargo test --test e2e_docker --features docker-tests -- --test-threads=1 --skip iceberg
fi

echo "✓ Tests passed!"

# Run DuckDB verification if Iceberg tests enabled
if [ "${TEST_ICEBERG:-0}" = "1" ]; then
  echo ""
  echo "Running DuckDB verification..."
  if command -v duckdb &> /dev/null; then
    ./scripts/verify-duckdb.sh
  else
    echo "⚠️  DuckDB not installed - skipping verification"
    echo "    Install: brew install duckdb (macOS) or https://duckdb.org/docs/installation/"
  fi
fi
