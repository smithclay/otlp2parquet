# Phase 8: Documentation Overhaul Plan

**Date:** 2025-01-16
**Related:** [icepick Integration Design](./2025-01-16-icepick-integration-design.md)

## Overview

Comprehensive documentation update to reflect the icepick integration architecture. Focus on operator/SRE audience with practical deployment guides, while updating contributor documentation for accuracy.

**Primary Audience:** Operators and SREs deploying otlp2parquet
**Secondary Audience:** Developers and contributors

**Strategy:** Update setup guides with new capabilities and configuration, refresh architecture docs, maintain existing TOC structure under docs/

---

## 1. README.md Updates

**Goal:** High-level overview highlighting new capabilities with links to detailed docs

### Changes

- Update feature list to emphasize icepick-powered Iceberg support
- Highlight S3 Tables catalog support for AWS Lambda
- Showcase R2 Data Catalog support for Cloudflare Workers
- Update "Quick Start" to reflect current deployment reality
- Ensure all links point to updated docs/ structure
- Keep concise - 2-3 screens maximum

### New Capabilities to Highlight

- Simplified architecture (single `otlp2parquet-writer` crate)
- Native S3 Tables support via ARN configuration
- R2 Data Catalog support for Cloudflare Workers
- Better catalog abstraction via icepick
- Improved WASM binary size (1.3MB compressed, 43.8% of limit)

### Success Criteria

- ✅ All links work and point to correct docs
- ✅ Feature list accurately reflects current capabilities
- ✅ Quick start is actionable and correct
- ✅ Length under 200 lines

---

## 2. Setup Guides (docs/setup/)

**Goal:** Practical, copy-paste deployment instructions for operators

### docs/setup/aws-lambda.md

**Updates:**
- Replace old Iceberg REST config with S3 Tables ARN approach
- Show complete environment variable setup for Lambda
- Document ARN format: `arn:aws:s3tables:region:account-id:bucket/bucket-name`
- Example configuration:
  ```bash
  OTLP2PARQUET_BUCKET_ARN=arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket
  ```
- Document required IAM permissions for S3 Tables
- Update deployment commands to use `make build-lambda`
- Add troubleshooting section for common S3 Tables issues
- Include working example from `examples/aws-lambda-s3-tables/`

**IAM Permissions to Document:**
- `s3tables:GetTable`
- `s3tables:CreateTable`
- `s3tables:UpdateTable`
- `s3:PutObject`, `s3:GetObject` (for data files)
- Lake Formation permissions if enabled

### docs/setup/cloudflare.md

**Updates:**
- Update R2 configuration for R2 Data Catalog support
- Show wrangler.toml configuration with R2 catalog bindings
- Document R2 bucket setup and catalog initialization
- Include environment variable configuration for R2
- Emphasize WASM binary size achievement (1.3MB compressed)
- Update build commands to use `make wasm-full`
- Show catalog-enabled vs plain Parquet deployment options
- Include working example configuration

**Example Configuration:**
```toml
[[r2_buckets]]
binding = "OTLP_BUCKET"
bucket_name = "otlp-data"

[vars]
OTLP2PARQUET_CATALOG_TYPE = "r2"
```

### docs/setup/docker.md

**Updates:**
- Verify docker-compose.yml examples still work
- Update to show optional Nessie catalog integration
- Keep as reference for local development and testing
- Ensure MinIO + Nessie example is current

**Success Criteria:**
- ✅ All setup guides have been tested end-to-end
- ✅ Configuration examples are copy-paste ready
- ✅ Troubleshooting sections cover common issues
- ✅ IAM/permissions documented clearly

---

## 3. AGENTS.md Updates

**Goal:** Accurate workspace architecture and development guidelines for AI agents and contributors

### Workspace Architecture Section

**Updates:**
- Update crate list:
  - ✅ **Keep:** `otlp2parquet-core`, `otlp2parquet-batch`, `otlp2parquet-config`, `otlp2parquet-proto`
  - ✅ **Add:** `otlp2parquet-writer` - Unified writer with icepick integration
  - ❌ **Remove:** `otlp2parquet-storage`, `otlp2parquet-iceberg` (deleted crates)
- Document icepick as external dependency
- Update crate responsibility descriptions:
  - `otlp2parquet-writer`: Unified writer abstraction with catalog support via icepick
  - Platform-specific: `cloudflare`, `lambda`, `server`
- Clarify architecture: Single writer crate vs previous multi-crate approach

**New Section Content:**
```markdown
6. **Workspace Architecture**
   - `otlp2parquet-core` - Schema definitions and core types
   - `otlp2parquet-batch` - In-memory batching logic
   - `otlp2parquet-config` - Configuration parsing and defaults
   - `otlp2parquet-writer` - Unified writer with icepick integration
   - `otlp2parquet-proto` - OTLP protobuf definitions
   - Platform-specific: `cloudflare`, `lambda`, `server`
   - External: `icepick` - Iceberg/Parquet writer abstraction
```

### Development Guidelines Section

**New Content to Add:**
- Section on working with icepick integration
- Document the `OtlpWriter` trait pattern
- Explain catalog initialization per platform
- Note WASM constraints:
  - S3 Tables catalog not available on WASM (Lambda-only)
  - R2 Data Catalog available on WASM (Cloudflare)
- Add guidance on testing:
  - E2E tests: `make test-e2e` (core tests)
  - E2E with Iceberg: `make test-e2e-iceberg`
  - WASM build: `make wasm-full`
  - WASM size check: `make wasm-size`
- Document warn-and-succeed catalog pattern (failures don't block writes)

**Example Addition:**
```markdown
### Working with icepick

The `otlp2parquet-writer` crate provides a unified `OtlpWriter` trait implemented by `IcepickWriter`. This abstraction handles:
- Parquet file writing via icepick
- Optional catalog commits (S3 Tables, Nessie, R2 Data Catalog)
- Platform-specific initialization
- Warn-and-succeed pattern for catalog failures

Catalog operations are non-blocking - write failures are logged but don't prevent Parquet file creation.
```

### Keep Existing

- Schema specifications (unchanged)
- Partition path formats (unchanged)
- Signal type handling (unchanged)
- Commit message conventions (unchanged)
- Apache Iceberg Integration section (mostly current)

**Success Criteria:**
- ✅ Crate list is accurate
- ✅ Architecture reflects current implementation
- ✅ Development guidelines cover icepick patterns
- ✅ Testing instructions are complete

---

## 4. Concepts Documentation (docs/concepts/)

**Goal:** Light updates to reflect icepick integration where relevant

### docs/concepts/architecture.md

**Updates:**
- Update architecture description to show icepick layer
- Simplify explanation: "single writer crate with icepick" vs "separate storage/iceberg crates"
- Show data flow: OTLP → Arrow → icepick → Parquet/Catalog
- Clarify platform differences:
  - Lambda: S3 Tables catalog
  - Cloudflare: R2 Data Catalog
  - Server: Flexible (Nessie, plain Parquet, etc.)
- Add architecture diagram if helpful (optional)

**Data Flow to Document:**
```
OTLP Request (protobuf/JSON/JSONL)
    ↓
Parse & Convert to Arrow RecordBatch
    ↓
Batch Accumulation
    ↓
icepick Writer
    ├─→ Write Parquet to object storage
    └─→ Optional: Commit to Iceberg catalog
```

### docs/concepts/configuration.md

**Updates:**
- Document new environment variables:
  - `OTLP2PARQUET_BUCKET_ARN` - S3 Tables bucket ARN (Lambda)
  - Platform detection variables (existing: `AWS_LAMBDA_FUNCTION_NAME`, `CF_WORKER`)
- Remove deprecated Iceberg REST config vars
- Add configuration precedence rules:
  1. Environment variables
  2. config.toml
  3. Platform-specific defaults
- Document platform detection logic
- Include catalog type options:
  - S3 Tables (Lambda via ARN)
  - Nessie (Server via REST endpoint)
  - R2 Data Catalog (Cloudflare)
  - Plain Parquet (all platforms, no catalog)

### docs/concepts/storage.md

**Updates:**
- Emphasize icepick as the storage abstraction layer
- Clarify catalog modes:
  - **With catalog:** ACID transactions, schema evolution, time travel
  - **Plain Parquet:** Simple object storage writes, no metadata management
- Document warn-and-succeed pattern:
  - Catalog failures are logged as warnings
  - Parquet writes continue regardless of catalog state
  - Ensures data durability even if catalog is unavailable
- Update to reflect OpenDAL usage (via icepick)

### Leave Mostly Unchanged

- `docs/concepts/schema.md` - Schema definitions haven't changed

**Success Criteria:**
- ✅ Architecture accurately reflects icepick integration
- ✅ Configuration docs cover all new env vars
- ✅ Storage docs explain catalog vs plain Parquet modes
- ✅ Platform differences are clear

---

## 5. Additional Documentation Tasks

### New Reference Documentation (docs/reference/)

#### Create docs/reference/environment-variables.md

Complete reference table for all `OTLP2PARQUET_*` environment variables.

**Structure:**
- Table format with columns: Variable, Platforms, Required, Default, Description
- Organized by category: Storage, Catalog, Batch, Server

**Example Entries:**

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_BUCKET_ARN` | Lambda | Yes | - | S3 Tables bucket ARN for catalog |
| `OTLP2PARQUET_CATALOG_TYPE` | Cloudflare, Server | No | `plain` | Catalog type: `s3-tables`, `nessie`, `r2`, `plain` |
| `OTLP2PARQUET_BATCH_SIZE` | All | No | `1000` | Max records per batch |

**Categories to Cover:**
- Platform detection (automatic)
- Storage configuration
- Catalog configuration
- Batch processing
- Server-specific (HTTP port, auth)

#### Create docs/reference/catalog-types.md

Detailed guide for each catalog type.

**Sections:**

**S3 Tables (Lambda)**
- ARN format and parsing
- Required IAM permissions
- Setup instructions
- Example configuration
- Troubleshooting

**Nessie (Server/Docker)**
- REST endpoint configuration
- Authentication setup
- Namespace configuration
- Example docker-compose setup
- Troubleshooting

**R2 Data Catalog (Cloudflare Workers)**
- R2 bucket setup
- wrangler.toml configuration
- Catalog bindings
- Example configuration
- Limitations and considerations
- Troubleshooting

**Plain Parquet (All Platforms)**
- When to use (no catalog overhead)
- Performance characteristics
- Query engine compatibility
- Example configuration

### Update Existing Guides (docs/guides/)

#### docs/guides/sending-data.md

**Review:**
- Verify all examples still work (should be unchanged)
- Confirm OTLP endpoints are correct
- Test curl examples

#### Create docs/guides/troubleshooting.md

**New Guide Structure:**

**Common Issues:**
1. **S3 Tables Permission Errors**
   - Symptom: "Access Denied" errors in Lambda logs
   - Cause: Missing IAM permissions
   - Solution: Add required permissions (list from setup guide)

2. **Catalog Connection Failures**
   - Symptom: Warning logs about catalog operations
   - Cause: Network issues, incorrect config, catalog unavailable
   - Solution: Verify configuration, check logs (data still written)

3. **WASM Deployment Size Issues**
   - Symptom: Cloudflare Workers deployment fails
   - Cause: Binary exceeds 3MB limit
   - Solution: Verify using `make wasm-size` (should be 1.3MB)

4. **Platform Detection Problems**
   - Symptom: Wrong initialization path taken
   - Cause: Environment variables set incorrectly
   - Solution: Check `AWS_LAMBDA_FUNCTION_NAME` and `CF_WORKER` vars

5. **Parquet File Not Created**
   - Symptom: No files in object storage
   - Cause: Storage credentials, bucket permissions
   - Solution: Verify storage configuration and IAM/credentials

**Debugging Tips:**
- Enable debug logging
- Check CloudWatch/Worker logs
- Verify storage bucket access
- Test catalog connectivity separately

### Documentation Quality Checks

**Tasks:**
- [ ] Run markdown link checker on all docs
- [ ] Verify all code examples are syntactically correct
- [ ] Test all setup guides end-to-end where possible
- [ ] Ensure consistent terminology:
  - "icepick" (lowercase) when referring to the library
  - "Catalog" (capitalized) when referring to Iceberg catalogs
  - "ARN" (all caps) for Amazon Resource Names
- [ ] Check for broken internal links
- [ ] Verify external links are current
- [ ] Spell check all documents

**Success Criteria:**
- ✅ Zero broken links
- ✅ All code examples tested
- ✅ Consistent terminology throughout
- ✅ Setup guides validated end-to-end

---

## Implementation Checklist

### Phase 8.1: Core Documentation (Priority 1)

- [ ] Update README.md with new capabilities
- [ ] Update docs/setup/aws-lambda.md with S3 Tables ARN config
- [ ] Update docs/setup/cloudflare.md with R2 Data Catalog support
- [ ] Update AGENTS.md workspace architecture section
- [ ] Update AGENTS.md development guidelines section

### Phase 8.2: Reference Documentation (Priority 2)

- [ ] Create docs/reference/environment-variables.md
- [ ] Create docs/reference/catalog-types.md
- [ ] Create docs/guides/troubleshooting.md
- [ ] Update docs/concepts/architecture.md
- [ ] Update docs/concepts/configuration.md
- [ ] Update docs/concepts/storage.md

### Phase 8.3: Quality Assurance (Priority 3)

- [ ] Run link checker
- [ ] Test all code examples
- [ ] Verify setup guides end-to-end
- [ ] Check terminology consistency
- [ ] Spell check all documents

---

## Success Metrics

| Metric | Target | Status |
|--------|--------|--------|
| README.md updated | Complete | ⏳ |
| Setup guides updated | 3/3 | ⏳ |
| AGENTS.md updated | Complete | ⏳ |
| Reference docs created | 2/2 | ⏳ |
| Concepts updated | 3/3 | ⏳ |
| Troubleshooting guide created | Complete | ⏳ |
| Broken links | 0 | ⏳ |
| Code examples tested | 100% | ⏳ |

---

## Notes

- Focus on operator/SRE audience first, contributor docs second
- Emphasize practical examples and copy-paste configurations
- Highlight new capabilities (R2 catalog, S3 Tables, improved WASM size)
- No migration documentation needed (fresh start)
- Maintain existing docs/ TOC structure
- Keep README high-level, detailed content in docs/
