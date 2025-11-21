# Lambda + S3 Tables Smoke Tests

Smoke tests for otlp2parquet Lambda function with AWS S3 Tables Iceberg catalog.

## Overview

The Lambda smoke test suite validates the full pipeline:
1. **Deploy**: SAM CLI automatically packages and deploys Lambda function + S3 Tables bucket
2. **Send signals**: POST OTLP payloads to Lambda Function URL
3. **Verify execution**: Query CloudWatch Logs for errors
4. **Verify data**: DuckDB queries S3 Tables catalog + S3 Parquet files
5. **Cleanup**: Delete CloudFormation stack

## Prerequisites

### Required

- AWS credentials configured (IAM role with Lambda and S3 Tables permissions)
- Lambda deployment package built: `make build-lambda`
- AWS SAM CLI installed (automatically via `uvx` if using Makefile)

### IAM Permissions

The CI/CD role or your AWS credentials need:

**Lambda**:
- `lambda:CreateFunction`
- `lambda:DeleteFunction`
- `lambda:InvokeFunction`
- `lambda:CreateFunctionUrlConfig`

**CloudFormation**:
- `cloudformation:CreateStack`
- `cloudformation:DeleteStack`
- `cloudformation:DescribeStacks`
- `cloudformation:GetTemplate`

**S3 Tables**:
- `s3tables:CreateTableBucket`
- `s3tables:DeleteTableBucket`
- `s3tables:CreateNamespace`
- `s3tables:CreateTable`
- `s3tables:GetTable`
- `s3tables:UpdateTableMetadataLocation`

**S3**:
- `s3:PutObject` (upload Lambda ZIP)
- `s3:GetObject` (read Lambda ZIP)
- `s3:ListBucket`

**CloudWatch Logs**:
- `logs:CreateLogGroup`
- `logs:DescribeLogStreams`
- `logs:FilterLogEvents`

**IAM**:
- `iam:CreateRole`
- `iam:DeleteRole`
- `iam:AttachRolePolicy`
- `iam:PassRole`

## Running Locally

### Environment Variables

```bash
# Optional - all have sensible defaults
export SMOKE_TEST_STACK_PREFIX=smoke-lambda       # Default stack name prefix
export SMOKE_TEST_AWS_REGION=us-west-2            # AWS region for deployment
export SMOKE_TEST_S3_TABLES_NAMESPACE=otel_smoke  # Iceberg namespace

# Note: No deployment bucket needed - SAM CLI manages this automatically
```

### Run Tests

```bash
# Build Lambda binary
make build-lambda

# Run all Lambda smoke tests
make smoke-lambda

# Or run with cargo directly
cargo test --test smoke_tests --features smoke-lambda -- lambda --test-threads=1
```

### Expected Output

```
==> Running Lambda smoke tests...
test lambda_tests::lambda_s3_tables_full_pipeline ... ok
test lambda_tests::lambda_logs_only ... ok

test result: ok. 2 passed; 0 failed
```

## Running in CI

The smoke tests run automatically on:
- **Push to main branch**: Always runs
- **Pull requests**: Only when PR has `test-lambda` label

### GitHub Secrets Required

```yaml
AWS_SMOKE_TEST_ROLE_ARN: arn:aws:iam::123456789012:role/GitHubActionsRole
AWS_SMOKE_TEST_DEPLOYMENT_BUCKET: my-deployment-bucket
```

### Trigger on PR

Add the `test-lambda` label to your PR to trigger smoke tests.

## Test Cases

### `lambda_s3_tables_full_pipeline`

**Full end-to-end validation**:
- Deploys Lambda function with S3 Tables catalog
- Sends logs, metrics (gauge), and traces
- Verifies no errors in CloudWatch Logs
- Uses DuckDB to verify:
  - Tables exist (`otel_logs`, `otel_traces`, `otel_metrics_gauge`)
  - Row counts > 0
  - Schema correctness
- Cleans up all resources

### `lambda_logs_only`

**Minimal signal type test**:
- Deploys Lambda function
- Sends only logs (protobuf format)
- Verifies execution succeeded
- Cleans up resources

## Architecture

### CloudFormation Template

Located at `scripts/smoke/lambda-template.yaml`:

```yaml
Resources:
  S3TablesBucket:         # Managed Iceberg tables
  S3TablesNamespace:      # Namespace 'otlp'
  LambdaExecutionRole:    # IAM role with s3tables permissions
  OtlpIngestFunction:     # Lambda function (ARM64, 512MB, 60s timeout)
  OtlpFunctionUrl:        # HTTP endpoint for OTLP ingestion
```

**Stack outputs**:
- `FunctionUrl`: Lambda Function URL for sending OTLP data
- `S3TablesBucketArn`: S3 Tables bucket ARN
- `DataBucket`: Underlying S3 bucket name

### DuckDB Verification

The smoke tests use DuckDB to query S3 Tables:

```sql
-- Attach S3 Tables catalog
ATTACH 'iceberg://s3tables' AS iceberg_catalog (
    TYPE ICEBERG,
    CATALOG_TYPE 's3tables',
    TABLE_BUCKET_ARN 'arn:aws:s3tables:...'
);

-- List tables
SELECT table_name FROM iceberg_catalog.otel_smoke.information_schema.tables;

-- Count rows
SELECT COUNT(*) FROM iceberg_catalog.otel_smoke.otel_logs;
```

## Debugging

### View CloudFormation Stack

```bash
aws cloudformation describe-stacks \
  --stack-name smoke-lambda-abc123 \
  --region us-west-2
```

### View Lambda Logs

```bash
aws logs tail /aws/lambda/smoke-lambda-abc123-ingest \
  --follow \
  --region us-west-2
```

### Query S3 Tables Directly

```bash
# List tables
aws s3tables list-tables \
  --table-bucket-arn arn:aws:s3tables:us-west-2:123456789012:bucket/otlp2parquet-smoke \
  --namespace otel_smoke

# Get table metadata
aws s3tables get-table-metadata-location \
  --table-bucket-arn arn:aws:s3tables:us-west-2:123456789012:bucket/otlp2parquet-smoke \
  --namespace otel_smoke \
  --name otel_logs
```

### Manual Cleanup

If tests fail and don't cleanup automatically:

```bash
# Delete CloudFormation stack
aws cloudformation delete-stack \
  --stack-name smoke-lambda-abc123 \
  --region us-west-2

# Wait for deletion
aws cloudformation wait stack-delete-complete \
  --stack-name smoke-lambda-abc123 \
  --region us-west-2
```

## Troubleshooting

### Error: "SMOKE_TEST_DEPLOYMENT_BUCKET not set"

**Solution**: Set the environment variable:
```bash
export SMOKE_TEST_DEPLOYMENT_BUCKET=my-bucket
```

### Error: "Stack creation failed"

**Possible causes**:
- S3 Tables bucket name already exists (must be globally unique)
- Insufficient IAM permissions
- Lambda ZIP not uploaded to S3

**Solution**: Check CloudFormation stack events:
```bash
aws cloudformation describe-stack-events \
  --stack-name smoke-lambda-abc123 \
  --region us-west-2
```

### Error: "Lambda execution had errors"

**Solution**: Check CloudWatch Logs:
```bash
aws logs filter-log-events \
  --log-group-name /aws/lambda/smoke-lambda-abc123-ingest \
  --filter-pattern "ERROR" \
  --region us-west-2
```

### DuckDB Verification Fails

**Possible causes**:
- Tables don't exist yet (Lambda timed out)
- S3 Tables catalog not accessible
- DuckDB not installed

**Solution**:
1. Check Lambda succeeded: `aws lambda invoke`
2. Verify S3 Tables: `aws s3tables list-tables`
3. Install DuckDB: `brew install duckdb`
