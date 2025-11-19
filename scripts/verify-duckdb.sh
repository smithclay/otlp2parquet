#!/bin/bash
set -euo pipefail

#
# DuckDB Iceberg verification script
#
# Verifies that Iceberg tables were created and contain data by querying via DuckDB
#
# Requirements:
# - DuckDB CLI installed (v1.1.0+)
# - Nessie catalog running (for Iceberg metadata)
# - MinIO running (for Parquet data)
#
# Usage:
#   ./scripts/verify-duckdb.sh                    # Verify all tables
#   NESSIE_URI=http://localhost:19120 ./scripts/verify-duckdb.sh
#

NESSIE_URI="${NESSIE_URI:-http://localhost:19120}"
CATALOG_ENDPOINT="${NESSIE_URI}/iceberg"
NAMESPACE="${ICEBERG_NAMESPACE:-otlp}"

# Check if duckdb is available
if ! command -v duckdb &> /dev/null; then
  echo "⚠️  DuckDB not installed. Installing via Homebrew..."
  if command -v brew &> /dev/null; then
    brew install duckdb
  else
    echo "ERROR: Please install DuckDB manually: https://duckdb.org/docs/installation/"
    exit 1
  fi
fi

echo "========================================="
echo "DuckDB Iceberg Verification"
echo "========================================="
echo "Nessie base endpoint: http://localhost:19120/iceberg"
echo "Warehouse: warehouse"
echo "Namespace: ${NAMESPACE}"
echo ""
echo "DuckDB will query: http://localhost:19120/iceberg/v1/config?warehouse=warehouse"
echo "Nessie will return prefix for branch/warehouse routing"
echo ""

# Expected tables based on schema definitions
EXPECTED_TABLES=(
  "otel_logs"
  "otel_traces"
  "otel_metrics_gauge"
  "otel_metrics_sum"
  "otel_metrics_histogram"
  "otel_metrics_exponential_histogram"
  "otel_metrics_summary"
)

# Create a temporary DuckDB verification script
VERIFY_SQL=$(mktemp /tmp/duckdb_verify_XXXXXX.sql)
trap "rm -f $VERIFY_SQL" EXIT

cat > "$VERIFY_SQL" <<'EOF'
--
-- DuckDB Iceberg Verification for otlp2parquet
--
-- This script uses DuckDB's ATTACH to connect to the Nessie REST catalog
-- and verifies that:
-- 1. The Nessie REST catalog is accessible
-- 2. All expected OTLP tables were created
-- 3. Each table contains data
--

-- Install and load required extensions
INSTALL httpfs;
INSTALL iceberg;
LOAD httpfs;
LOAD iceberg;

-- Create S3 secret for accessing Parquet files in MinIO
CREATE SECRET minio_secret (
  TYPE S3,
  KEY_ID 'minioadmin',
  SECRET 'minioadmin',
  REGION 'us-east-1',
  ENDPOINT 'localhost:9000',
  URL_STYLE 'path',
  USE_SSL false
);

-- Attach Nessie REST catalog
-- DuckDB will call: http://localhost:19120/iceberg/v1/config?warehouse=warehouse
-- Nessie returns prefix: "main%7Cwarehouse" which DuckDB uses for subsequent calls
-- The warehouse name 'warehouse' matches nessie.catalog.default-warehouse in docker-compose.yml
-- AUTHORIZATION_TYPE 'none' since our test Nessie doesn't require auth
ATTACH 'warehouse' AS nessie_catalog (
  TYPE ICEBERG,
  ENDPOINT 'http://localhost:19120/iceberg',
  AUTHORIZATION_TYPE 'none'
);

-- Set display modes
.mode box
.headers on

-- Verify catalog connectivity
SELECT '=== Testing Iceberg REST Catalog Connection ===' AS status;
SELECT 'Endpoint: CATALOG_ENDPOINT_PLACEHOLDER' AS info;
SELECT 'Namespace: NAMESPACE_PLACEHOLDER' AS info;
SELECT '' AS status;

-- Show all tables in the catalog
SELECT '=== Tables in Nessie Catalog ===' AS status;
SHOW ALL TABLES;
SELECT '' AS status;

-- Verify each expected table exists and has data
SELECT '=== Row Counts ===' AS status;
EOF

# Add row count queries for each expected table using attached catalog
for table in "${EXPECTED_TABLES[@]}"; do
  cat >> "$VERIFY_SQL" <<EOF
SELECT '${table}' AS table_name, COUNT(*) AS row_count
FROM nessie_catalog.${NAMESPACE}.${table}
UNION ALL
EOF
done

# Remove trailing UNION ALL and add semicolon
if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' '$ s/UNION ALL/;/' "$VERIFY_SQL"
else
  sed -i '$ s/UNION ALL/;/' "$VERIFY_SQL"
fi

cat >> "$VERIFY_SQL" <<EOF

-- Sample data from each table type
SELECT '' AS status;
SELECT '=== Sample from otel_logs (first 3 rows) ===' AS status;
SELECT * FROM nessie_catalog.${NAMESPACE}.otel_logs LIMIT 3;

SELECT '' AS status;
SELECT '=== Sample from otel_traces (first 3 rows) ===' AS status;
SELECT * FROM nessie_catalog.${NAMESPACE}.otel_traces LIMIT 3;

SELECT '' AS status;
SELECT '=== Sample from otel_metrics_gauge (first 3 rows) ===' AS status;
SELECT * FROM nessie_catalog.${NAMESPACE}.otel_metrics_gauge LIMIT 3;
EOF

# Replace placeholders
if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' "s|CATALOG_ENDPOINT_PLACEHOLDER|${CATALOG_ENDPOINT}|g" "$VERIFY_SQL"
  sed -i '' "s|NAMESPACE_PLACEHOLDER|${NAMESPACE}|g" "$VERIFY_SQL"
else
  sed -i "s|CATALOG_ENDPOINT_PLACEHOLDER|${CATALOG_ENDPOINT}|g" "$VERIFY_SQL"
  sed -i "s|NAMESPACE_PLACEHOLDER|${NAMESPACE}|g" "$VERIFY_SQL"
fi

echo "Running DuckDB verification queries..."
echo ""

# Save DuckDB output to a file for debugging
DUCKDB_OUTPUT=$(mktemp /tmp/duckdb_output_XXXXXX.log)
trap "rm -f $VERIFY_SQL $DUCKDB_OUTPUT" EXIT

# Run DuckDB with the verification script
if duckdb < "$VERIFY_SQL" 2>&1 | tee "$DUCKDB_OUTPUT"; then
  echo ""
  echo "✓ DuckDB verification passed!"
  exit 0
else
  EXIT_CODE=$?
  echo ""
  echo "❌ DuckDB verification failed with exit code $EXIT_CODE"
  echo ""
  echo "Debugging information:"
  echo "  Catalog endpoint: ${CATALOG_ENDPOINT}"
  echo "  Namespace: ${NAMESPACE}"
  echo ""
  echo "  Test connection manually:"
  echo "    duckdb -c \"INSTALL httpfs; INSTALL iceberg; LOAD httpfs; LOAD iceberg; CREATE SECRET minio_secret (TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin', REGION 'us-east-1', ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false); ATTACH 'warehouse' AS nessie_catalog (TYPE ICEBERG, ENDPOINT 'http://localhost:19120/iceberg', AUTHORIZATION_TYPE 'none'); SHOW ALL TABLES;\""
  echo ""
  echo "  Check Nessie catalog health:"
  echo "    curl http://localhost:19120/api/v2/config"
  echo "    curl ${CATALOG_ENDPOINT}/v1/config"
  echo ""
  echo "  List tables via REST API:"
  echo "    curl ${CATALOG_ENDPOINT}/v1/main/namespaces/${NAMESPACE}/tables"
  echo ""
  echo "Possible causes:"
  echo "  - Tables don't exist yet (no data was written by otlp2parquet)"
  echo "  - Nessie catalog is unreachable (check: docker compose logs nessie)"
  echo "  - MinIO/S3 credentials incorrect (check docker-compose.yml)"
  echo "  - Wrong Nessie branch (should be 'main')"
  echo "  - Iceberg metadata files missing in MinIO s3://otlp-logs/warehouse/"
  exit $EXIT_CODE
fi
