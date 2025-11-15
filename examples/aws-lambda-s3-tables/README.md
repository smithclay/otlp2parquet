# AWS Lambda + S3 Tables Example

Production-ready deployment of `otlp2parquet` as an AWS Lambda function with S3 Tables (Apache Iceberg).

## Quick Start

Deploy in 4 commands:

```bash
# 1. Setup Glue integration (one-time)
./lifecycle.sh setup --region us-west-2

# 2. Deploy the Lambda stack
./lifecycle.sh deploy --region us-west-2

# 3. Test with sample data
./lifecycle.sh test --region us-west-2

# 4. View logs
./lifecycle.sh logs --region us-west-2 --follow
```

## What's Included

- **`template.yaml`** - CloudFormation template creating:
  - S3 Table Bucket (Iceberg storage)
  - Lambda function (arm64)
  - IAM execution role
  - CloudWatch log group

- **`lifecycle.sh`** - Unified workflow script:
  - `setup` Glue/Lake Formation integration
  - `deploy` Lambda package + CloudFormation stack
  - `test` OTLP fixtures (logs, traces, metrics)
  - `logs` tailing + `status` outputs + `delete` cleanup

## Lifecycle Script

`lifecycle.sh` consolidates the previous helper scripts into a single CLI:

| Command | Purpose | Common flags |
| --- | --- | --- |
| `setup` | Create Glue namespace and Lake Formation grants | `--region`, `--stack-name`, `--database` |
| `deploy` | Download/upload Lambda zip and deploy CloudFormation | `--region`, `--stack-name`, `--arch`, `--version`, `--local-binary` |
| `test` | Invoke Lambda with bundled OTLP fixtures and show summary | `--stack-name`, `--region`, `--verbose` |
| `logs` | Tail `/aws/lambda/<function>` logs | `--stack-name`, `--region`, `--since`, `--follow` |
| `status` | Persist/print CloudFormation outputs | `--stack-name`, `--region` |
| `delete` | Delete the stack and optional deployment bucket | `--stack-name`, `--region`, `--force`, `--retain-bucket` |

Each subcommand validates prerequisites (AWS CLI, jq, unzip) and prints the next recommended step.

## Prerequisites

- AWS CLI configured (`aws configure`)
- Permissions to create: Lambda, S3 Tables, IAM roles, CloudFormation stacks
- `jq` installed for JSON parsing

## Setup: Integrate S3 Tables with Glue Data Catalog

Before deploying, integrate your S3 Tables bucket with AWS Glue:

```bash
# Run one-time setup
./lifecycle.sh setup --region us-west-2
```

This configures:
- S3 Tables integration with Glue Data Catalog
- Namespace (database) creation in Glue
- Lake Formation permissions for Lambda role

**Why Glue?** AWS S3 Tables REST endpoint doesn't support snapshot management
(add-snapshot operations). AWS Glue provides full Iceberg REST catalog support,
which is the same pattern AWS Firehose uses.

## Architecture

```
Lambda → Glue Iceberg REST → Glue Data Catalog → S3 Tables
         (metadata API)      (snapshots/schema)   (Parquet storage)
```

## Usage

### Deploy

1. **Setup Glue integration** (one-time):
   ```bash
   ./lifecycle.sh setup --region us-west-2
   ```

2. **Deploy Lambda**:
   ```bash
   # Deploy with defaults (arm64, us-west-2)
    ./lifecycle.sh deploy --region us-west-2

   # Deploy with specific options
    ./lifecycle.sh deploy --arch amd64 --region us-west-2 --stack-name my-otlp-stack

   # Deploy specific version
    ./lifecycle.sh deploy --version v0.0.2

   # Deploy locally built binary (for development)
    ./lifecycle.sh deploy --local-binary ../../target/lambda/bootstrap/bootstrap.zip
   ```

3. **Test ingestion**:
   ```bash
   ./lifecycle.sh test --region us-west-2
   ```

### Test

```bash
# Run all tests
./lifecycle.sh test

# Test specific stack
./lifecycle.sh test --stack-name my-otlp-stack --region us-west-2

# Verbose output
./lifecycle.sh test --verbose
```

### Cleanup

```bash
# Delete stack and resources
./lifecycle.sh delete --region us-west-2
```

## How It Works

The Lambda function:
1. Receives OTLP data (protobuf/JSON)
2. Converts to Arrow RecordBatch
3. Writes Parquet to S3 Table Bucket
4. Commits Iceberg transaction via REST catalog

Tables created automatically:
- `otel.logs`
- `otel.traces`
- `otel.metrics_gauge`
- `otel.metrics_sum`
- `otel.metrics_histogram`
- `otel.metrics_exponential_histogram`
- `otel.metrics_summary`

## Operating Modes

The Lambda function supports two operating modes:

### Mode 1: S3 Tables + Glue Data Catalog (Recommended)

Writes Parquet files to S3 Tables with Iceberg metadata managed via AWS Glue Data Catalog.

**Environment Variables:**
```bash
OTLP2PARQUET_ICEBERG_REST_URI=https://glue.us-west-2.amazonaws.com/iceberg
OTLP2PARQUET_ICEBERG_WAREHOUSE=123456789012  # Your AWS account ID (Glue catalog ID)
OTLP2PARQUET_ICEBERG_NAMESPACE=otel
OTLP2PARQUET_STORAGE_BACKEND=s3
OTLP2PARQUET_S3_REGION=us-west-2
```

**Benefits:**
- Atomic write-and-commit operations
- ACID transactions for data consistency
- Automatic table creation from Arrow schemas
- Query via DuckDB, Spark, Athena with Iceberg catalog
- Schema evolution and time travel
- Efficient metadata management

**When to use:**
- Production deployments requiring data consistency
- Multi-user query scenarios with concurrent reads/writes
- Long-term data retention with schema evolution
- Integration with modern data lake ecosystems

### Mode 2: Plain S3 without Iceberg (Legacy)

Writes Parquet files to regular S3 bucket without catalog integration.

**Environment Variables:**
```bash
OTLP2PARQUET_STORAGE_BACKEND=s3
OTLP2PARQUET_S3_BUCKET=my-parquet-bucket
OTLP2PARQUET_S3_REGION=us-west-2
```

**Benefits:**
- Simple S3 storage without catalog overhead
- Direct file access for custom tooling
- Lower complexity for simple use cases

**When to use:**
- Simple archival scenarios
- Custom query pipelines that don't need Iceberg
- Cost optimization for write-once, read-rarely patterns
- Development and testing environments

### Switching Between Modes

To switch modes, update the `Environment.Variables` section in `template.yaml`:

**For S3 Tables mode:** Keep the `OTLP2PARQUET_ICEBERG_*` variables set
**For Plain S3 mode:** Comment out `OTLP2PARQUET_ICEBERG_*` variables

The Lambda function automatically detects which mode to use based on the presence of `OTLP2PARQUET_ICEBERG_REST_URI`.

## Querying Data

### DuckDB

```bash
duckdb -c "
  INSTALL iceberg;
  LOAD iceberg;

  CREATE SECRET (
    TYPE S3,
    REGION 'us-west-2'
  );

  -- Query via Glue catalog
  SELECT COUNT(*) FROM iceberg_scan('glue://otel.logs', aws_region='us-west-2');
"
```

### AWS Athena

```sql
-- Tables automatically available in Athena
SELECT COUNT(*) FROM otel.logs;
SELECT COUNT(*) FROM otel.traces;
SELECT COUNT(*) FROM otel.metrics_gauge;
```

## Production Extensions

This example is for testing and development. For production, add:

- **API Gateway** - HTTPS endpoint for OTLP clients
- **Authentication** - API keys or IAM-based auth
- **Monitoring** - CloudWatch dashboards and alarms
- **VPC** - If accessing private resources
- **Provisioned Concurrency** - Reduce cold start latency

## Documentation

See [docs/storage/aws-lambda-s3-tables.md](../../docs/storage/aws-lambda-s3-tables.md) for:
- Detailed architecture explanation
- Configuration reference
- Production considerations
- Troubleshooting guide
- Cost optimization tips

## Cost Estimate

Approximate costs for 1 million OTLP requests/month:

- Lambda invocations: ~$0.20
- Lambda compute (arm64, 512MB): ~$1.00
- S3 Tables storage (10GB): ~$0.23
- S3 Tables API requests: ~$0.05
- CloudWatch Logs (1GB): ~$0.50

**Total: ~$2/month** (excluding data transfer and queries)

## Troubleshooting

**Lambda timeout errors**
- Increase `Timeout` in `template.yaml`
- Increase `MemorySize` for faster execution

**IAM permission errors**
- Check CloudWatch logs: `aws logs tail /aws/lambda/otlp2parquet-ingest`
- Verify IAM role has S3 Tables permissions

**Missing tables**
- Tables are created on first write (not pre-created)
- Check Lambda logs for Iceberg commit errors
- Verify environment variables in CloudFormation

## License

Same as otlp2parquet project.
