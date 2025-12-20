-- DuckDB query script for Cloudflare R2 Data Catalog
-- Requires DuckDB 1.4.0+ with iceberg and httpfs extensions
--
-- Usage:
--   duckdb < query-r2-catalog.sql
--
-- Replace the placeholders below with your R2 credentials:
--   <ACCESS_KEY_ID>   - R2 S3-compatible API access key
--   <SECRET_KEY>      - R2 S3-compatible API secret key
--   <ACCOUNT_ID>      - Cloudflare account ID (32-char hex)
--   <BUCKET_NAME>     - R2 bucket name with Data Catalog enabled
--   <CATALOG_TOKEN>   - R2 API token with Data Catalog permissions

INSTALL iceberg;
LOAD iceberg;
INSTALL httpfs;
LOAD httpfs;

-- R2 S3-compatible API credentials for reading Parquet files
CREATE SECRET r2_storage (
    TYPE S3,
    KEY_ID '<ACCESS_KEY_ID>',
    SECRET '<SECRET_KEY>',
    ENDPOINT '<ACCOUNT_ID>.r2.cloudflarestorage.com',
    REGION 'auto',
    URL_STYLE 'path'
);

-- R2 Data Catalog token for Iceberg REST catalog
CREATE SECRET r2_catalog (
    TYPE ICEBERG,
    TOKEN '<CATALOG_TOKEN>'
);

-- Attach R2 Data Catalog (warehouse = {account_id}_{bucket_name})
ATTACH '<ACCOUNT_ID>_<BUCKET_NAME>' AS r2 (
    TYPE ICEBERG,
    ENDPOINT 'https://catalog.cloudflarestorage.com/<ACCOUNT_ID>/<BUCKET_NAME>'
);

-- List tables
SHOW ALL TABLES;

-- Count records per signal type
SELECT 'otel_logs' as table_name, count(*) as rows FROM r2.otlp.otel_logs
UNION ALL SELECT 'otel_traces', count(*) FROM r2.otlp.otel_traces;

-- Example queries (uncomment to use):

-- Recent logs
-- SELECT Timestamp, ServiceName, SeverityText, Body
-- FROM r2.otlp.otel_logs
-- ORDER BY Timestamp DESC LIMIT 10;

-- Slow spans (>100ms)
-- SELECT ServiceName, SpanName, Duration/1e6 as ms
-- FROM r2.otlp.otel_traces
-- WHERE Duration > 100000000
-- ORDER BY Duration DESC LIMIT 10;
