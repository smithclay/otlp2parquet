# Docs Simplification Design

> **Status:** Planned

**Goal:** Radically simplify docs around the new CLI `deploy` command, following Stripe-style task-oriented structure.

## New Structure: 5 Pages

| Page | Purpose |
|------|---------|
| `index.md` | Hook + "Get Started" link |
| `deploying.md` | Run `otlp2parquet deploy` (CF/AWS/Docker tabs) |
| `sending-data.md` | Point OTel at your endpoint |
| `querying.md` | Query with DuckDB |
| `reference.md` | Env vars, schema, config (one page) |

## mkdocs.yml Navigation

```yaml
nav:
  - 'Home': 'index.md'
  - 'Deploy': 'deploying.md'
  - 'Send Data': 'sending-data.md'
  - 'Query Data': 'querying.md'
  - 'Reference': 'reference.md'
  - 'Query Demo': 'https://smithclay.github.io/otlp2parquet/query-demo/'
```

## Page Content Outlines

### index.md - Home

- One-liner: what this does
- 30-second value prop (cheap storage, serverless, query with DuckDB)
- "Get Started" button linking to `deploying.md`
- No tutorial - just the hook

### deploying.md - Deploy

- Starts with `otlp2parquet deploy <platform>` as the primary path
- Three tabs: Cloudflare / AWS / Docker
- Each tab contains:
  - The deploy command + what it does
  - "Next steps" output from CLI (secrets, deploy command)
  - Collapsible "Manual setup" for power users
  - Collapsible "Production considerations" (auth, batching, monitoring)
  - Collapsible "Troubleshooting"

### sending-data.md - Send Data

- Point OTel SDK/Collector at your endpoint
- Examples: curl, OTel Collector config, SDK snippets
- Sections for logs, traces, metrics

### querying.md - Query Data

- DuckDB examples (primary)
- Iceberg vs plain Parquet query differences
- Brief mentions of Athena, Spark, etc.

### reference.md - Reference

- All env vars in one table
- Schema tables (logs, traces, metrics)
- Catalog modes explained
- Config file format (for server mode)

## Migration Plan

### Files to Remove/Merge

| Current Location | Action |
|------------------|--------|
| `setup/overview.md` | Remove |
| `setup/aws-lambda.md` | Merge into `deploying.md` |
| `setup/cloudflare.md` | Merge into `deploying.md` |
| `setup/docker.md` | Merge into `deploying.md` |
| `setup/cli.md` | Merge into `deploying.md` |
| `concepts/architecture.md` | Remove (internal detail) |
| `concepts/schema.md` | Merge into `reference.md` |
| `concepts/configuration.md` | Merge into `reference.md` |
| `concepts/storage.md` | Remove (internal detail) |
| `guides/index.md` | Remove |
| `guides/sending-data.md` | Becomes `sending-data.md` |
| `guides/querying-data.md` | Becomes `querying.md` |
| `guides/troubleshooting.md` | Merge into `deploying.md` collapsibles |
| `guides/lambda-local-development.md` | Merge into `deploying.md` AWS section |
| `guides/plain-s3.md` | Merge into `deploying.md` or `reference.md` |
| `guides/building-from-source.md` | Move to `CONTRIBUTING.md` |
| `reference/environment-variables.md` | Merge into `reference.md` |
| `reference/catalog-types.md` | Merge into `reference.md` |
| `contributing/*` | Move to `CONTRIBUTING.md` in repo root |
| `testing/*` | Move to `CONTRIBUTING.md` in repo root |
| `plans/*` | Keep but exclude from nav (internal) |

### Directories to Delete After Migration

- `setup/`
- `concepts/`
- `guides/`
- `reference/`
- `contributing/`
- `testing/`

## Key Principles

1. **CLI-first**: `deploy` command is the happy path, not manual template editing
2. **Task-oriented**: Deploy -> Send -> Query (Stripe-style)
3. **Progressive disclosure**: Collapsibles for advanced/troubleshooting content
4. **Single reference page**: No hunting across multiple files for config/schema/env vars
5. **Flat structure**: No subdirectories, ~5 files total

## Reduction

- **Before:** ~25 markdown files across 7 directories
- **After:** 5 markdown files, flat structure
