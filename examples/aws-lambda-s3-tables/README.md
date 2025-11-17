# AWS Lambda + S3 Tables Example

Production-ready deployment of `otlp2parquet` as an AWS Lambda function with AWS S3 Tables (Apache Iceberg).

## Quick Start

Deploy in 3 commands:

```bash
# 1. Deploy the Lambda stack
./lifecycle.sh deploy --region us-west-2

# 2. Test with sample data
./lifecycle.sh test --region us-west-2

# 3. View logs
./lifecycle.sh logs --region us-west-2 --follow
```

## What's Included

- **`template.yaml`** - CloudFormation template creating:
  - S3 Tables Bucket (managed Iceberg catalog + storage)
  - S3 Tables Namespace (`otel`)
  - Lambda function (arm64)
  - IAM execution role with S3 Tables permissions
  - CloudWatch log group

- **`lifecycle.sh`** - Unified workflow script:
  - `deploy` Lambda package + CloudFormation stack
  - `test` OTLP fixtures (logs, traces, metrics)
  - `logs` tailing + `status` outputs + `delete` cleanup

## Lifecycle Script

`lifecycle.sh` provides a single CLI for managing the deployment:

| Command | Purpose | Common flags |
| --- | --- | --- |
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

## Architecture

```
Lambda → S3 Tables Catalog API → S3 Tables (Parquet storage + metadata)
         (create/update tables)    (managed Iceberg catalog)
```

**Key Components:**

- **S3 Tables Bucket**: Managed service combining Iceberg catalog + object storage
- **S3 Tables Namespace**: Logical grouping (`otel`) for all telemetry tables
- **Lambda Function**: Processes OTLP data and writes to S3 Tables via icepick
- **IAM Role**: Grants `s3tables:*` and `s3:*` permissions

## Usage

### Deploy

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
3. Writes Parquet files to S3 Tables
4. Commits Iceberg table metadata via S3 Tables API

Tables created automatically on first write:
- `otel.logs`
- `otel.traces`
- `otel.metrics_gauge`
- `otel.metrics_sum`
- `otel.metrics_histogram`
- `otel.metrics_exponential_histogram`
- `otel.metrics_summary`

## Configuration

The Lambda uses these environment variables (set in `template.yaml`):

```bash
# Storage backend
OTLP2PARQUET_STORAGE_BACKEND=s3
OTLP2PARQUET_S3_REGION=us-west-2

# S3 Tables Iceberg integration (ARN-based)
OTLP2PARQUET_ICEBERG_BUCKET_ARN=arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket

# Logging
RUST_LOG=info
```

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

  -- Query via S3 Tables
  SELECT COUNT(*) FROM iceberg_scan('s3tables://your-bucket/otel/logs');
"
```

### AWS Athena

S3 Tables are automatically available in Athena:

```sql
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
- Verify IAM role has `s3tables:*` and `s3:*` permissions

**Missing tables**
- Tables are created on first write (not pre-created)
- Check Lambda logs for S3 Tables API errors
- Verify `OTLP2PARQUET_ICEBERG_BUCKET_ARN` is correctly set

**S3 Tables bucket not found**
- Ensure the S3 Tables bucket and namespace were created by CloudFormation
- Check stack outputs: `./lifecycle.sh status`

## License

Same as otlp2parquet project.
