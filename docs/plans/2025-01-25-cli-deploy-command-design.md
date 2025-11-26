# CLI Deploy Command Design

## Overview

A new `deploy` subcommand that generates platform-specific deployment configs through an interactive wizard. Templates are embedded in the binary via `include_str!`. The wizard guides first-time users through configuration, writes standard-named files, and prints the deployment command to run.

## Command Structure

```
otlp2parquet deploy <PLATFORM> [OPTIONS]

PLATFORMS:
  cloudflare    Generate wrangler.toml for Cloudflare Workers + R2
  aws           Generate template.yaml for AWS Lambda + S3/S3 Tables

COMMON OPTIONS:
  --bucket <NAME>       Pre-fill bucket name (skip prompt)
  --catalog <MODE>      Pre-fill catalog mode: "iceberg" or "none"
  --force               Overwrite existing file without asking
  --output <PATH>       Write to custom path instead of current directory

CLOUDFLARE OPTIONS:
  --account-id <ID>     Pre-fill Cloudflare Account ID
  --worker-name <NAME>  Pre-fill worker name

AWS OPTIONS:
  --stack-name <NAME>   Pre-fill CloudFormation stack name
  --lambda-s3-uri <URI> Pre-fill S3 URI of Lambda binary
  --retention <DAYS>    Log retention in days (default: 7)
```

Flags pre-fill wizard answers. If all required values are provided via flags, wizard runs non-interactively (useful for CI).

## Wizard Flows

### Cloudflare

```
$ otlp2parquet deploy cloudflare

otlp2parquet deploy - Cloudflare Workers + R2

? Worker name (default: nimble-relay-2847):
? R2 bucket name:
? Cloudflare Account ID:
? Enable Iceberg catalog?
  > No  - Plain Parquet files (simpler)
    Yes - R2 Data Catalog (queryable tables)

Created wrangler.toml

Next steps:
  1. Create R2 bucket (if needed):
     wrangler r2 bucket create <bucket-name>

  2. Set secrets:
     wrangler secret put AWS_ACCESS_KEY_ID
     wrangler secret put AWS_SECRET_ACCESS_KEY
     [if iceberg: wrangler secret put CLOUDFLARE_API_TOKEN]

  3. Deploy:
     wrangler deploy
```

### AWS

```
$ otlp2parquet deploy aws

otlp2parquet deploy - AWS Lambda + S3

? S3 URI of Lambda binary:
  > Example: s3://my-bucket/otlp2parquet-lambda-arm64.zip

  Download from: https://github.com/smithclay/otlp2parquet/releases/latest

? Stack name (default: cosmic-tracer-3847):
? S3 bucket name for data:
? Enable Iceberg catalog?
  > No  - Plain Parquet to S3
    Yes - S3 Tables

? CloudWatch log retention (days): 7

Created template.yaml

Next steps:
  1. Deploy:
     aws cloudformation deploy \
       --template-file template.yaml \
       --stack-name <stack-name> \
       --capabilities CAPABILITY_IAM
```

## Fun Default Names

Generated format: `{adjective}-{noun}-{4-digits}`

Examples: `swift-beacon-4821`, `plucky-parquet-9012`, `cosmic-tracer-3847`

### Wordlists (OTel/observability themed)

```rust
const ADJECTIVES: &[&str] = &[
    "swift", "eager", "bright", "cosmic", "dapper",
    "fluent", "golden", "humble", "jovial", "keen",
    "lively", "mellow", "nimble", "plucky", "quick",
    "rustic", "snappy", "trusty", "vivid", "witty",
];

const NOUNS: &[&str] = &[
    "arrow", "beacon", "conduit", "depot", "emitter",
    "funnel", "gauge", "harbor", "inlet", "journal",
    "keeper", "ledger", "metric", "nexus", "outlet",
    "parquet", "queue", "relay", "signal", "tracer",
];
```

## Implementation Details

### File Organization

```
crates/otlp2parquet-server/
├── src/
│   ├── main.rs              # Adds deploy subcommand
│   ├── lib.rs               # Existing server code
│   └── deploy/
│       ├── mod.rs           # Deploy command entry point
│       ├── cloudflare.rs    # Cloudflare wizard + template rendering
│       ├── aws.rs           # AWS wizard + template rendering
│       └── names.rs         # Fun name generation
└── templates/
    ├── wrangler.toml        # Embedded at compile time
    └── cloudformation.yaml  # Embedded at compile time
```

### Template Embedding

```rust
// deploy/cloudflare.rs
const WRANGLER_TEMPLATE: &str = include_str!("../../templates/wrangler.toml");

// deploy/aws.rs
const CFN_TEMPLATE: &str = include_str!("../../templates/cloudformation.yaml");
```

Templates use `{{PLACEHOLDER}}` syntax, rendered with simple `.replace()` calls. No templating engine needed.

### Dependencies

Add to `crates/otlp2parquet-server/Cargo.toml`:

```toml
dialoguer = { version = "0.11", default-features = false }
```

Server crate only - no impact on Lambda/WASM binary size.

## Output Files

- **Cloudflare:** `wrangler.toml` (standard name, `wrangler deploy` finds it automatically)
- **AWS:** `template.yaml` (standard name for CloudFormation)

If file exists, warn and prompt to overwrite. `--force` flag skips the prompt.

## Security Considerations

- Credentials are NOT embedded in generated configs
- Cloudflare: User sets secrets via `wrangler secret put`
- AWS: IAM roles handle permissions, no credentials in template

## Catalog Mode Options

- **Cloudflare:** `none` (plain Parquet) or `iceberg` (R2 Data Catalog)
- **AWS:** `none` (plain Parquet to S3) or `iceberg` (S3 Tables)
