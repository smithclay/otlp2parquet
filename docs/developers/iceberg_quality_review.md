# Iceberg integration quality review

## Summary
- Normalized Iceberg data, manifest, and manifest-list paths to storage-relative keys so OpenDAL writes land in the warehouse layout expected by the catalog.【F:crates/otlp2parquet-iceberg/src/writer.rs†L48-L92】【F:crates/otlp2parquet-iceberg/src/manifest.rs†L56-L123】【F:crates/otlp2parquet-iceberg/src/catalog.rs†L508-L555】
- Reused the shared Parquet encoder from `otlp2parquet-core` so Iceberg commits inherit the same compression, hashing, and metadata tuning as the plain S3 writer.【F:crates/otlp2parquet-core/src/parquet/encoding.rs†L15-L148】【F:crates/otlp2parquet-iceberg/src/writer.rs†L94-L151】【F:crates/otlp2parquet-storage/src/parquet_writer.rs†L12-L96】
- Reduced Iceberg REST logging to debug-level snapshots and truncated payload excerpts, preventing multi-kilobyte table schemas from flooding production logs.【F:crates/otlp2parquet-iceberg/src/catalog.rs†L214-L249】【F:crates/otlp2parquet-iceberg/src/catalog.rs†L270-L318】【F:crates/otlp2parquet-iceberg/src/catalog.rs†L363-L405】
- Added an explicit `initialize_committer_with_config` path and plumbed runtime configuration through the server and Lambda entry points so deployments no longer need to duplicate Iceberg settings in two env namespaces.【F:crates/otlp2parquet-iceberg/src/init.rs†L72-L133】【F:crates/otlp2parquet-server/src/lib.rs†L20-L95】【F:crates/otlp2parquet-lambda/src/lib.rs†L10-L134】
- Removed manual table metadata fabrication—every write now consults the catalog after table creation attempts to honor catalog-managed locations.【F:crates/otlp2parquet-iceberg/src/writer.rs†L104-L157】

## Detailed updates

### 1. Storage paths now align with OpenDAL semantics
`IcebergWriter::generate_warehouse_path` now builds catalog paths with a helper that strips schemes, and `write_parquet` sends the storage-relative key to OpenDAL while preserving the catalog URI for Iceberg metadata. Manifest and manifest-list writers use the same helpers, and catalog commits stat files with relative keys to avoid zero-byte entries.【F:crates/otlp2parquet-iceberg/src/writer.rs†L48-L151】【F:crates/otlp2parquet-iceberg/src/manifest.rs†L56-L123】【F:crates/otlp2parquet-iceberg/src/catalog.rs†L508-L555】

### 2. Shared Parquet encoding
A new `otlp2parquet_core::parquet` module encapsulates the hashing buffer, writer properties, and metadata reconstruction. Both the storage writer and the Iceberg writer consume the same encoder, guaranteeing identical compression, row-group sizing, and metadata for every Parquet file regardless of commit path.【F:crates/otlp2parquet-core/src/parquet/encoding.rs†L15-L148】【F:crates/otlp2parquet-storage/src/parquet_writer.rs†L12-L96】【F:crates/otlp2parquet-iceberg/src/writer.rs†L118-L151】

### 3. Calmer REST logging
Create, load, and update operations now log payloads only at `debug!` and trim error bodies to 500 bytes, eliminating noisy info/warn logs while retaining diagnostics when debugging is enabled.【F:crates/otlp2parquet-iceberg/src/catalog.rs†L214-L249】【F:crates/otlp2parquet-iceberg/src/catalog.rs†L270-L318】【F:crates/otlp2parquet-iceberg/src/catalog.rs†L363-L405】

### 4. Configuration-driven initialization
`initialize_committer_with_config` allows runtimes to supply parsed `RuntimeConfig.iceberg` values directly. The server and Lambda adapters convert their runtime configuration into `IcebergRestConfig`, so deployments defined in TOML/ENV are honored without duplicating variables.【F:crates/otlp2parquet-iceberg/src/init.rs†L72-L133】【F:crates/otlp2parquet-server/src/lib.rs†L86-L139】【F:crates/otlp2parquet-lambda/src/lib.rs†L104-L173】

### 5. Catalog metadata is authoritative
`get_or_create_table` first attempts to load table metadata and, after any create attempt (including 409 conflicts), reloads the catalog instead of fabricating locations. This ensures Iceberg commits always use catalog-managed warehouse paths.【F:crates/otlp2parquet-iceberg/src/writer.rs†L104-L157】

## Remaining considerations
- The new path helpers currently operate on basic URI patterns; add integration tests for alternative catalog schemes (e.g., WASBS, GCS) to validate parsing edge cases.
- Consider exposing structured logging toggles so operators can opt into verbose REST traces without recompiling.
