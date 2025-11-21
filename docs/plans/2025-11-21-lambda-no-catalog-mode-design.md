# Lambda "No Catalog" Mode Support - Design Document

**Date**: 2025-11-21
**Status**: Design Complete
**Author**: AI Agent (brainstorming session)

## Overview

Add support for "no catalog" mode to the Lambda platform, enabling plain Parquet file writes to S3 without Apache Iceberg catalog integration. This aligns Lambda with the server and Cloudflare Workers platforms, which already support both catalog and no-catalog modes.

## Motivation

Currently, Lambda requires either S3 Tables or REST catalog configuration. Users who want simple Parquet files without catalog overhead have no option. Supporting "no catalog" mode provides:

- **Simpler deployment**: No S3 Tables bucket or catalog service required
- **Lower cost**: No catalog API calls or metadata storage costs
- **Flexibility**: Users can choose plain Parquet or Iceberg based on needs
- **Consistency**: All three platforms (server, Lambda, Cloudflare) support the same modes

## Design Decisions

### Configuration Approach

**Decision**: Use shared `config.catalog_mode` from `otlp2parquet-config` crate
**Rationale**: Aligns with server implementation, avoids duplicating catalog mode concept
**Alternative considered**: Lambda-specific catalog mode enum (rejected - adds divergence)

### Storage Configuration

**Decision**: Require explicit S3 storage config when `catalog_mode=none`
**Rationale**: Clear separation between catalog and storage, mirrors server behavior
**Alternative considered**: Infer from S3 Tables ARN (rejected - couples unrelated concepts)

### State Management

**Decision**: Make catalog optional in `LambdaState` struct
**Rationale**: Clean, mirrors the `write_batch` API which accepts `Option<&dyn Catalog>`
**Alternative considered**: Separate state types or no-op catalog (rejected - unnecessary complexity)

## Architecture

### Configuration Modes

Lambda will support three configuration modes:

#### Mode 1: S3 Tables (catalog_mode=iceberg, bucket_arn present)
- Current behavior, no changes
- Uses S3 Tables ARN: `arn:aws:s3tables:region:account:bucket/name`
- Storage is implicit in S3 Tables

#### Mode 2: REST Catalog (catalog_mode=iceberg, rest_uri present)
- Current behavior, no changes
- Requires explicit S3 config: `config.storage.s3.bucket` and `config.storage.s3.region`
- Creates OpenDAL S3 operator for storage

#### Mode 3: No Catalog (catalog_mode=none) - **NEW**
- Plain Parquet mode
- Requires explicit S3 config: `config.storage.s3.bucket` and `config.storage.s3.region`
- Creates OpenDAL S3 operator for storage
- No catalog initialization
- Files written directly to S3 with Hive partitioning

### Configuration Validation

- When `catalog_mode=iceberg`: Either `bucket_arn` or `rest_uri` must be present (current behavior)
- When `catalog_mode=none`: S3 storage config must be present, iceberg config is ignored
- Namespace is required in all modes (used for directory structure)

### Code Changes

#### LambdaState Struct

```rust
pub struct LambdaState {
    pub catalog: Option<Arc<dyn otlp2parquet_writer::icepick::catalog::Catalog>>,  // Changed from required
    pub namespace: String,
    pub passthrough: PassthroughBatcher,
    pub max_payload_bytes: usize,
}
```

**Note**: No separate storage operator field needed - the `otlp2parquet_writer` crate manages storage via global operator when catalog is `None`.

#### Handler Changes

In `handlers.rs` (process_logs, process_metrics, process_traces):

```rust
match otlp2parquet_writer::write_batch(
    state.catalog.as_deref(),  // Changed from Some(state.catalog.as_ref())
    &state.namespace,
    // ... rest of arguments
)
```

#### Initialization Flow

In `lib.rs::run()`:

```rust
// Check catalog_mode to determine initialization path
if config.catalog_mode == CatalogMode::Iceberg {
    // Initialize catalog (current behavior)
    let catalog = initialize_catalog(...).await?;
    // ...
} else {
    // catalog_mode == None
    // Initialize storage operator for plain Parquet writes
    otlp2parquet_writer::initialize_storage(&config)?;
    // catalog remains None
}
```

### Storage Operator Management

The `otlp2parquet_writer` crate already handles storage via a global operator:

- `otlp2parquet_writer::initialize_storage(&config)` - Initializes global operator
- `otlp2parquet_writer::write_batch(None, ...)` - Uses global operator for plain Parquet writes
- Lambda just needs to call `initialize_storage` when in "no catalog" mode

## Testing Strategy

### Unit Tests

In `crates/otlp2parquet-lambda/src/lib.rs`:

- Test configuration validation for all three modes
- Test that `catalog_mode=none` requires S3 storage config
- Test that `catalog_mode=iceberg` requires either `bucket_arn` or `rest_uri`
- Test error messages are clear and actionable

### Integration Tests

Create Lambda smoke test for "no catalog" mode:

- Similar to existing tests but with `OTLP2PARQUET_CATALOG_MODE=none`
- Verify Parquet files are written to S3 with correct partitioning
- Verify no catalog operations are attempted
- Verify files can be read by DuckDB/Athena

### Local Testing

Update SAM CLI local testing:

- Add `local-env.plain-parquet.json` example configuration
- Document MinIO setup for local testing (skip Localstack)
- Show how to verify files are written correctly

## Documentation Updates

### Files to Update

1. **docs/setup/aws-lambda.md**:
   - Add section "Running Without Iceberg Catalog"
   - Document `OTLP2PARQUET_CATALOG_MODE=none` configuration
   - Explain when to use plain Parquet mode vs S3 Tables
   - Add example configuration and DuckDB query patterns

2. **docs/concepts/configuration.md**:
   - Add `catalog_mode` to Lambda configuration section
   - Document that Lambda now supports same catalog modes as server
   - Update environment variable reference table

3. **docs/guides/lambda-local-development.md**:
   - Add example using MinIO for "no catalog" mode local testing
   - Show how to configure Lambda with `OTLP2PARQUET_CATALOG_MODE=none` + MinIO
   - Add commands to verify files are written correctly

4. **AGENTS.md** (project instructions):
   - Update Lambda section to mention "no catalog" mode support
   - Note that Lambda now aligns with server catalog mode behavior
   - Remove/update "Lambda: Always use icepick with S3 Tables" comments

5. **README.md** (if Lambda section exists):
   - Update to mention plain Parquet support

### Example Documentation Content

For aws-lambda.md, add section showing:

- **When to use plain Parquet**: Simpler deployments, lower cost, no catalog overhead
- **Configuration example**: Just S3 bucket/region, no S3 Tables
- **Query examples**: DuckDB and Athena reading plain Parquet from S3
- **Trade-offs**: No ACID, no schema evolution, no time travel vs simpler setup

## CloudFormation Template Updates

File: `crates/otlp2parquet-lambda/template.yaml`

### Add CatalogMode Parameter

```yaml
Parameters:
  CatalogMode:
    Type: String
    Default: iceberg
    AllowedValues:
      - iceberg
      - none
    Description: Catalog mode - 'iceberg' for S3 Tables, 'none' for plain Parquet
```

### Conditional Resource Creation

- **S3 Tables bucket**: Only create when `CatalogMode == iceberg`
- **IAM permissions**: Different policies based on catalog mode
  - With catalog: S3 Tables permissions + S3 data permissions
  - Without catalog: Only S3 data permissions

### Environment Variables

- **Always set**: `OTLP2PARQUET_CATALOG_MODE` (from parameter)
- **When iceberg**: Set `OTLP2PARQUET_ICEBERG_BUCKET_ARN`
- **When none**: Set `OTLP2PARQUET_STORAGE_S3_BUCKET` and `OTLP2PARQUET_STORAGE_S3_REGION`

### Outputs

- Add output showing data location (S3 bucket path)
- Add output with example DuckDB query for plain Parquet mode
- Keep existing S3 Tables outputs conditional on `CatalogMode == iceberg`

### Backward Compatibility

- Default `CatalogMode=iceberg` maintains current behavior
- Existing stacks continue working without changes
- No migration needed

## Example Configurations

### Plain Parquet Mode

```bash
# Environment variables
OTLP2PARQUET_CATALOG_MODE=none
OTLP2PARQUET_STORAGE_S3_BUCKET=my-otlp-bucket
OTLP2PARQUET_STORAGE_S3_REGION=us-west-2
```

### S3 Tables Mode (Current)

```bash
# Environment variables
OTLP2PARQUET_CATALOG_MODE=iceberg
OTLP2PARQUET_ICEBERG_BUCKET_ARN=arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket
```

### REST Catalog Mode

```bash
# Environment variables
OTLP2PARQUET_CATALOG_MODE=iceberg
OTLP2PARQUET_ICEBERG_REST_URI=https://glue.us-west-2.amazonaws.com/iceberg
OTLP2PARQUET_STORAGE_S3_BUCKET=my-otlp-bucket
OTLP2PARQUET_STORAGE_S3_REGION=us-west-2
```

## Implementation Checklist

- [ ] Update `LambdaState` struct to make catalog optional
- [ ] Update handlers to pass `state.catalog.as_deref()` to `write_batch`
- [ ] Update `lib.rs::run()` initialization logic to support `catalog_mode=none`
- [ ] Add unit tests for configuration validation
- [ ] Add integration test for plain Parquet mode
- [ ] Update CloudFormation template with `CatalogMode` parameter
- [ ] Add conditional resource creation in CloudFormation
- [ ] Create `local-env.plain-parquet.json` example
- [ ] Update `docs/setup/aws-lambda.md`
- [ ] Update `docs/concepts/configuration.md`
- [ ] Update `docs/guides/lambda-local-development.md`
- [ ] Update `AGENTS.md`
- [ ] Update `README.md` if applicable

## Migration Path

No migration required. This is a backward-compatible addition:

1. Existing deployments continue working with default `CatalogMode=iceberg`
2. New deployments can choose plain Parquet mode by setting parameter
3. Existing stacks can be updated to plain Parquet by updating parameter and redeploying

## Success Criteria

1. Lambda supports all three catalog modes (S3 Tables, REST, None)
2. Configuration validation provides clear error messages
3. Plain Parquet files are written to correct S3 paths with Hive partitioning
4. Documentation clearly explains when to use each mode
5. CloudFormation template supports both modes with sensible defaults
6. Tests verify all three modes work correctly
7. Local development with MinIO is documented and tested

## Future Considerations

- Schema registry support for plain Parquet mode
- Compaction service for plain Parquet files
- Migration tooling from plain Parquet to Iceberg
- Support for other catalog types (Nessie standalone, etc.)
