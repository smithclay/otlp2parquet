# AWS Lambda + S3 Tables Example

Production-ready deployment of `otlp2parquet` as an AWS Lambda function with S3 Tables (Apache Iceberg).

## Quick Start

Deploy in 3 commands:

```bash
# Deploy the stack
./deploy.sh --region us-east-1

# Test with sample data
./test.sh

# View logs
aws logs tail /aws/lambda/otlp2parquet-ingest --follow
```

## What's Included

- **`template.yaml`** - CloudFormation template creating:
  - S3 Table Bucket (Iceberg storage)
  - Lambda function (arm64)
  - IAM execution role
  - CloudWatch log group

- **`deploy.sh`** - Deployment automation:
  - Downloads Lambda binary from GitHub releases
  - Creates deployment S3 bucket
  - Uploads Lambda code
  - Deploys CloudFormation stack

- **`test.sh`** - Testing automation:
  - Invokes Lambda with sample OTLP data
  - Shows CloudWatch logs
  - Validates responses

## Prerequisites

- AWS CLI configured (`aws configure`)
- Permissions to create: Lambda, S3 Tables, IAM roles, CloudFormation stacks
- `jq` installed for JSON parsing

## Usage

### Deploy

```bash
# Deploy with defaults (arm64, us-west-2)
./deploy.sh

# Deploy with specific options
./deploy.sh --arch amd64 --region us-west-2 --stack-name my-otlp-stack

# Deploy specific version
./deploy.sh --version v0.0.2

# Deploy locally built binary (for development)
./deploy.sh --local-binary ../../target/lambda/bootstrap/bootstrap.zip
```

### Test

```bash
# Run all tests
./test.sh

# Test specific stack
./test.sh --stack-name my-otlp-stack --region us-west-2

# Verbose output
./test.sh --verbose
```

### Cleanup

```bash
# Delete stack and resources
./deploy.sh --delete
```

## Architecture

```
OTLP Client → Lambda (otlp2parquet) → S3 Tables (Iceberg)
                                          ↓
                                    Query Engines
                                    (DuckDB, Athena)
```

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

### Mode 1: S3 Tables with Iceberg (Recommended)

Writes Parquet files directly to S3 Tables warehouse with integrated Iceberg catalog.

**Environment Variables:**
```bash
OTLP2PARQUET_ICEBERG_REST_URI=https://s3tables.us-west-2.amazonaws.com/iceberg
OTLP2PARQUET_ICEBERG_WAREHOUSE=arn:aws:s3tables:us-west-2:123456789012:bucket/otlp2parquet
OTLP2PARQUET_ICEBERG_NAMESPACE=otel
OTLP2PARQUET_STORAGE_BACKEND=s3
OTLP2PARQUET_S3_BUCKET=my-data-bucket
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

## Query Examples

### DuckDB

```sql
INSTALL iceberg;
LOAD iceberg;

-- Configure S3 Tables catalog
CREATE SECRET s3tables (
    TYPE ICEBERG_REST,
    ENDPOINT 'https://s3tables.us-east-1.amazonaws.com/iceberg',
    WAREHOUSE 'arn:aws:s3tables:us-east-1:123456789012:bucket/otlp2parquet-demo',
    AWS_REGION 'us-east-1'
);

-- Query logs
SELECT * FROM iceberg_scan('s3tables', 'otel', 'logs')
WHERE Timestamp > now() - INTERVAL 1 HOUR
LIMIT 100;
```

### Amazon Athena

```sql
SELECT * FROM "s3tables_catalog"."otel"."logs"
WHERE Timestamp > current_timestamp - INTERVAL '1' HOUR
LIMIT 100;
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
