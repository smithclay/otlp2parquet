# Cloudflare Workers Smoke Test Setup

## Overview

The Workers smoke tests use a **simplified ephemeral approach**:
- ✅ **Persistent**: One R2 bucket + credentials (manual one-time setup)
- ✅ **Ephemeral**: Unique test prefix per run + Worker deployment
- ✅ **Cleanup**: Deletes objects with test prefix (bucket remains)

This matches the Lambda pattern: persistent infrastructure, ephemeral test data.

## One-Time Setup

### 1. Create R2 Bucket

```bash
# Via Cloudflare Dashboard
https://dash.cloudflare.com → R2 → Create bucket
Bucket name: otlp2parquet-smoke
```

### 2. Create R2 API Token

```bash
# Via Cloudflare Dashboard
https://dash.cloudflare.com → R2 → Manage R2 API Tokens

Permissions: Admin Read & Write (account-wide)
Copy: Access Key ID + Secret Access Key
```

### 3. Configure Environment

```bash
cd crates/otlp2parquet-cloudflare
cp .env.example .env

# Edit .env with your values:
CLOUDFLARE_API_TOKEN=your_api_token
CLOUDFLARE_ACCOUNT_ID=your_account_id
CLOUDFLARE_BUCKET_NAME=otlp2parquet-smoke
AWS_ACCESS_KEY_ID=your_r2_access_key_id
AWS_SECRET_ACCESS_KEY=your_r2_secret_access_key
# AWS_ENDPOINT_URL=https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com  # Optional
```

## Running Tests

```bash
# Build WASM
make wasm-compress

# Run all Workers smoke tests
make smoke-workers

# Run specific test
cargo test --test smoke_tests --features smoke-workers -- workers_plain_r2_full_pipeline
cargo test --test smoke_tests --features smoke-workers -- workers_r2_catalog_full_pipeline
cargo test --test smoke_tests --features smoke-workers -- workers_metrics_only
```

## Test Flow

For each test run:

1. **Setup**:
   - Generate unique test ID (e.g., `abc123`)
   - Create test prefix: `smoke-abc123/`
   - Generate wrangler config for unique Worker name

2. **Deploy**:
   - Enable R2 Data Catalog (if catalog mode)
   - Deploy Worker with bucket + credentials
   - Worker writes to `{bucket}/smoke-abc123/*`

3. **Test**:
   - Send OTLP signals to Worker
   - Verify execution via Worker logs
   - Verify data in R2 (optional: via DuckDB + catalog)

4. **Cleanup** (fail-safe):
   - Delete Worker script
   - Delete all objects with prefix `smoke-abc123/`
   - Bucket and credentials remain for next test

## Test Modes

### Plain R2 (No Catalog)
- Worker writes Parquet files directly to R2
- No Iceberg catalog metadata
- Test: `workers_plain_r2_full_pipeline`

### R2 Data Catalog (Iceberg)
- Enables R2 Data Catalog for bucket
- Worker writes Parquet + Iceberg metadata
- Queryable via DuckDB Iceberg connector
- Test: `workers_r2_catalog_full_pipeline`

## Isolation

- ✅ **Bucket level**: Shared bucket (pre-created)
- ✅ **Prefix level**: Unique per test (`smoke-{uuid}/`)
- ✅ **Worker level**: Unique deployment (`smoke-workers-{uuid}`)
- ✅ **Parallel safe**: Different prefixes don't conflict

## Troubleshooting

### "Failed to create R2 bucket"
This error should not occur anymore - bucket is pre-created and reused.

### "AWS_ACCESS_KEY_ID not set"
Create `.env` file from `.env.example` with real R2 S3 API credentials.

### "Unauthorized" errors
Verify R2 API token has "Admin Read & Write" permissions.

### Objects remain after test
Check Worker logs - cleanup may have failed. Manually delete via:
```bash
npx wrangler r2 object list otlp2parquet-smoke --prefix smoke-{uuid}/
npx wrangler r2 object delete otlp2parquet-smoke/{key}
```

## Comparison to Lambda Tests

| Aspect | Lambda | Workers |
|--------|--------|---------|
| **Persistent** | AWS Account + Credentials | R2 Bucket + Credentials |
| **Ephemeral** | CloudFormation Stack | Test Prefix + Worker |
| **Isolation** | Stack name | Prefix within bucket |
| **Cleanup** | Delete stack | Delete prefixed objects |
| **Setup** | AWS CLI configured | `.env` file |
