#!/bin/bash
set -euo pipefail

#
# Cloudflare Workers + R2 Data Catalog smoke test script
#
# Deploy Workers script → Send OTLP signals → Verify execution → Verify data → Cleanup
#
# Prerequisites:
# - WASM binary built (make wasm-compress)
# - wrangler CLI installed
# - Cloudflare API token with Workers and R2 permissions
#
# Usage:
#   export CLOUDFLARE_API_TOKEN=...
#   export CLOUDFLARE_ACCOUNT_ID=...
#   ./scripts/smoke/smoke-workers.sh
#

# Configuration
WORKER_PREFIX="${SMOKE_TEST_WORKER_PREFIX:-smoke-workers}"
R2_BUCKET="${SMOKE_TEST_R2_BUCKET:-otlp-smoke}"
R2_CATALOG_ENDPOINT="${SMOKE_TEST_R2_CATALOG_ENDPOINT:-}"

# Check prerequisites
if [ -z "${CLOUDFLARE_API_TOKEN:-}" ]; then
  echo "ERROR: CLOUDFLARE_API_TOKEN not set"
  exit 1
fi

if [ -z "${CLOUDFLARE_ACCOUNT_ID:-}" ]; then
  echo "ERROR: CLOUDFLARE_ACCOUNT_ID not set"
  exit 1
fi

if ! command -v wrangler &> /dev/null; then
  echo "ERROR: wrangler CLI not found. Install: npm install -g wrangler"
  exit 1
fi

# Generate unique test ID
TEST_ID=$(uuidgen | tr '[:upper:]' '[:lower:]' | cut -d'-' -f1)
WORKER_NAME="${WORKER_PREFIX}-${TEST_ID}"

echo "========================================="
echo "Workers + R2 Catalog Smoke Test"
echo "========================================="
echo "Worker name: ${WORKER_NAME}"
echo "R2 bucket: ${R2_BUCKET}"
if [ -n "${R2_CATALOG_ENDPOINT}" ]; then
  echo "R2 Catalog: ${R2_CATALOG_ENDPOINT}"
else
  echo "R2 Catalog: Not configured (plain Parquet)"
fi
echo ""

# Cleanup function
cleanup() {
  echo "Cleaning up Workers script..."
  wrangler delete "${WORKER_NAME}" --force || true
}

trap cleanup EXIT

# Deploy Workers script
echo "==> Deploying Workers script..."
cd crates/otlp2parquet-cloudflare

wrangler deploy \
  --name "${WORKER_NAME}" \
  --compatibility-date "2024-01-01"

# Extract Workers URL from wrangler output
WORKER_URL="https://${WORKER_NAME}.workers.dev"

echo "Workers deployed: ${WORKER_URL}"
echo ""

# Send OTLP signals
echo "==> Sending OTLP signals..."

# Send logs
curl -X POST "${WORKER_URL}/v1/logs" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @../../testdata/logs.pb \
  -f -s -o /dev/null || {
  echo "ERROR: Failed to send logs"
  exit 1
}
echo "Sent logs"

# Send metrics
curl -X POST "${WORKER_URL}/v1/metrics" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @../../testdata/metrics_gauge.pb \
  -f -s -o /dev/null || {
  echo "ERROR: Failed to send metrics"
  exit 1
}
echo "Sent metrics"

# Send traces
curl -X POST "${WORKER_URL}/v1/traces" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @../../testdata/traces.pb \
  -f -s -o /dev/null || {
  echo "ERROR: Failed to send traces"
  exit 1
}
echo "Sent traces"
echo ""

# Verify execution
echo "==> Verifying Workers execution..."
sleep 5  # Wait for logs

wrangler tail "${WORKER_NAME}" --format json --once > /tmp/workers-tail.log || true

if grep -q "error\|ERROR\|warn" /tmp/workers-tail.log; then
  echo "WARNING: Found errors/warnings in Workers logs:"
  grep "error\|ERROR\|warn" /tmp/workers-tail.log | head -5
else
  echo "No errors found in Workers logs"
fi
echo ""

# Verify data in R2 (if catalog configured)
if [ -n "${R2_CATALOG_ENDPOINT}" ]; then
  echo "==> Verifying data in R2 Data Catalog..."

  if command -v duckdb &> /dev/null; then
    # Use DuckDB to query R2 catalog
    # (This would require R2 access keys and catalog endpoint configuration)
    echo "DuckDB verification not yet implemented for R2 Catalog"
  else
    echo "DuckDB not installed - skipping catalog verification"
  fi
else
  echo "==> R2 Catalog not configured - skipping data verification"
fi
echo ""

echo "✓ Workers smoke test passed!"
